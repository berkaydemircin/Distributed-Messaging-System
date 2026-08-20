package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

var _ raft.FSM = (*ControllerFSM)(nil)

// BrokerInfo tracks a registered broker.
type BrokerInfo struct {
	ID            int32
	IncarnationID string
	BrokerEpoch   uint64
	Fenced        bool
}

// TopicState marks a topic as existing, independent of its partitions.
type TopicState struct {
	Name string
}

// PartitionState is a partition's controller-owned metadata.
type PartitionState struct {
	LeaderBrokerID int32
	LeaderEpoch    int32
	PartitionEpoch int32
	Replicas       []int32
	ISR            []int32
}

// TopicPartitionKey identifies a single partition.
type TopicPartitionKey struct {
	Topic     string
	Partition int32
}

type controllerState struct {
	Brokers    map[int32]BrokerInfo
	Topics     map[string]TopicState
	Partitions map[TopicPartitionKey]PartitionState
}

func newControllerState() controllerState {
	return controllerState{
		Brokers:    make(map[int32]BrokerInfo),
		Topics:     make(map[string]TopicState),
		Partitions: make(map[TopicPartitionKey]PartitionState),
	}
}

// deepCopy returns an independent state copy.
func (s controllerState) deepCopy() controllerState {
	out := newControllerState()
	for id, b := range s.Brokers {
		out.Brokers[id] = b
	}
	for name, t := range s.Topics {
		out.Topics[name] = t
	}
	for key, p := range s.Partitions {
		cp := p
		cp.Replicas = append([]int32(nil), p.Replicas...)
		cp.ISR = append([]int32(nil), p.ISR...)
		out.Partitions[key] = cp
	}
	return out
}

type CommandType string

const (
	CommandRegisterBroker  CommandType = "RegisterBroker"
	CommandUnfenceBroker   CommandType = "UnfenceBroker"
	CommandCreateTopic     CommandType = "CreateTopic"
	CommandUpdatePartition CommandType = "UpdatePartition"
)

// CommandEnvelopeVersion1 is the only currently supported envelope version.
const CommandEnvelopeVersion1 = 1

// CommandEnvelope wraps a versioned controller command.
type CommandEnvelope struct {
	Version int             `json:"version"`
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// EncodeCommand encodes a command for Raft.Apply.
func EncodeCommand(cmdType CommandType, payload interface{}) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("controller: encode %s payload: %w", cmdType, err)
	}
	env := CommandEnvelope{Version: CommandEnvelopeVersion1, Type: cmdType, Payload: body}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("controller: encode envelope: %w", err)
	}
	return out, nil
}

// RegisterBrokerCommand registers a broker incarnation.
type RegisterBrokerCommand struct {
	ID            int32  `json:"id"`
	IncarnationID string `json:"incarnation_id"`
}

// UnfenceBrokerCommand activates a registered broker.
type UnfenceBrokerCommand struct {
	BrokerID    int32  `json:"broker_id"`
	BrokerEpoch uint64 `json:"broker_epoch"`
}

// CreateTopicPartitionSpec is an initial partition assignment.
type CreateTopicPartitionSpec struct {
	Partition      int32   `json:"partition"`
	LeaderBrokerID int32   `json:"leader_broker_id"`
	Replicas       []int32 `json:"replicas"`
	ISR            []int32 `json:"isr"`
}

// CreateTopicCommand creates a topic with explicit assignments.
type CreateTopicCommand struct {
	Topic      string                     `json:"topic"`
	Partitions []CreateTopicPartitionSpec `json:"partitions"`
}

// UpdatePartitionCommand atomically updates partition metadata.
type UpdatePartitionCommand struct {
	Topic                  string  `json:"topic"`
	Partition              int32   `json:"partition"`
	ExpectedPartitionEpoch int32   `json:"expected_partition_epoch"`
	LeaderBrokerID         int32   `json:"leader_broker_id"`
	Replicas               []int32 `json:"replicas"`
	ISR                    []int32 `json:"isr"`
	// AllowUncleanElection permits a leader outside the previous ISR.
	AllowUncleanElection bool `json:"allow_unclean_election"`
}

type ControllerFSM struct {
	mu    sync.RWMutex
	state controllerState
}

func NewControllerFSM() *ControllerFSM {
	return &ControllerFSM{state: newControllerState()}
}

// Apply applies one committed controller command.
func (f *ControllerFSM) Apply(log *raft.Log) interface{} {
	if log == nil {
		return fmt.Errorf("controller fsm: nil log")
	}

	var env CommandEnvelope
	if err := json.Unmarshal(log.Data, &env); err != nil {
		return fmt.Errorf("controller fsm: decode envelope: %w", err)
	}
	if env.Version != CommandEnvelopeVersion1 {
		return fmt.Errorf("controller fsm: unsupported command envelope version %d", env.Version)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch env.Type {
	case CommandRegisterBroker:
		return f.applyRegisterBroker(env.Payload, log.Index)
	case CommandUnfenceBroker:
		return f.applyUnfenceBroker(env.Payload)
	case CommandCreateTopic:
		return f.applyCreateTopic(env.Payload)
	case CommandUpdatePartition:
		return f.applyUpdatePartition(env.Payload)
	default:
		return fmt.Errorf("controller fsm: unknown command type %q", env.Type)
	}
}

func (f *ControllerFSM) applyRegisterBroker(payload json.RawMessage, logIndex uint64) error {
	var cmd RegisterBrokerCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return fmt.Errorf("register broker: decode: %w", err)
	}
	if cmd.ID < 0 {
		return fmt.Errorf("register broker: invalid broker id %d", cmd.ID)
	}
	if cmd.IncarnationID == "" {
		return fmt.Errorf("register broker: incarnation id required")
	}
	if logIndex == 0 {
		return fmt.Errorf("register broker: invalid broker epoch 0")
	}

	if existing, ok := f.state.Brokers[cmd.ID]; ok {
		if existing.IncarnationID == cmd.IncarnationID {
			return nil
		}
		if !existing.Fenced || brokerInActivePartition(f.state.Partitions, cmd.ID) {
			return fmt.Errorf("register broker: broker %d still has an active incarnation", cmd.ID)
		}
	}

	f.state.Brokers[cmd.ID] = BrokerInfo{
		ID:            cmd.ID,
		IncarnationID: cmd.IncarnationID,
		BrokerEpoch:   logIndex,
		Fenced:        true,
	}
	return nil
}

func (f *ControllerFSM) applyUnfenceBroker(payload json.RawMessage) error {
	var cmd UnfenceBrokerCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return fmt.Errorf("unfence broker: decode: %w", err)
	}

	b, ok := f.state.Brokers[cmd.BrokerID]
	if !ok {
		return fmt.Errorf("unfence broker: unknown broker %d", cmd.BrokerID)
	}
	if b.BrokerEpoch != cmd.BrokerEpoch {
		return fmt.Errorf("unfence broker: expected broker epoch %d, current is %d", cmd.BrokerEpoch, b.BrokerEpoch)
	}

	b.Fenced = false
	f.state.Brokers[cmd.BrokerID] = b
	return nil
}

func brokerInActivePartition(partitions map[TopicPartitionKey]PartitionState, brokerID int32) bool {
	for _, partition := range partitions {
		if partition.LeaderBrokerID == brokerID || containsInt32(partition.ISR, brokerID) {
			return true
		}
	}
	return false
}

func (f *ControllerFSM) applyCreateTopic(payload json.RawMessage) error {
	var cmd CreateTopicCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return fmt.Errorf("create topic: decode: %w", err)
	}
	if cmd.Topic == "" {
		return fmt.Errorf("create topic: topic name required")
	}
	if len(cmd.Partitions) == 0 {
		return fmt.Errorf("create topic: at least one partition required")
	}
	if _, exists := f.state.Topics[cmd.Topic]; exists {
		return fmt.Errorf("create topic: topic %q already exists", cmd.Topic)
	}

	// Validate every partition before mutating anything.
	seen := make(map[int32]bool, len(cmd.Partitions))
	for _, spec := range cmd.Partitions {
		if spec.Partition < 0 {
			return fmt.Errorf("create topic: invalid partition %d", spec.Partition)
		}
		if seen[spec.Partition] {
			return fmt.Errorf("create topic: duplicate partition %d in request", spec.Partition)
		}
		seen[spec.Partition] = true
		if err := validatePartitionSpec(f.state.Brokers, spec.LeaderBrokerID, spec.Replicas, spec.ISR); err != nil {
			return fmt.Errorf("create topic: partition %d: %w", spec.Partition, err)
		}
	}
	for partition := int32(0); partition < int32(len(cmd.Partitions)); partition++ {
		if !seen[partition] {
			return fmt.Errorf("create topic: partitions must be contiguous from 0")
		}
	}

	f.state.Topics[cmd.Topic] = TopicState{Name: cmd.Topic}
	for _, spec := range cmd.Partitions {
		key := TopicPartitionKey{Topic: cmd.Topic, Partition: spec.Partition}
		f.state.Partitions[key] = PartitionState{
			LeaderBrokerID: spec.LeaderBrokerID,
			LeaderEpoch:    0,
			PartitionEpoch: 0,
			Replicas:       append([]int32(nil), spec.Replicas...),
			ISR:            append([]int32(nil), spec.ISR...),
		}
	}
	return nil
}

func (f *ControllerFSM) applyUpdatePartition(payload json.RawMessage) error {
	var cmd UpdatePartitionCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return fmt.Errorf("update partition: decode: %w", err)
	}

	key := TopicPartitionKey{Topic: cmd.Topic, Partition: cmd.Partition}
	current, ok := f.state.Partitions[key]
	if !ok {
		return fmt.Errorf("update partition: partition %s/%d does not exist", cmd.Topic, cmd.Partition)
	}
	if cmd.ExpectedPartitionEpoch != current.PartitionEpoch {
		return fmt.Errorf("update partition: expected partition epoch %d, current is %d", cmd.ExpectedPartitionEpoch, current.PartitionEpoch)
	}
	if err := validatePartitionSpec(f.state.Brokers, cmd.LeaderBrokerID, cmd.Replicas, cmd.ISR); err != nil {
		return fmt.Errorf("update partition: %w", err)
	}

	leaderChanged := cmd.LeaderBrokerID != current.LeaderBrokerID
	if leaderChanged && !containsInt32(current.ISR, cmd.LeaderBrokerID) && !cmd.AllowUncleanElection {
		return fmt.Errorf("update partition: leader %d was outside the previous ISR %v; unclean election not requested",
			cmd.LeaderBrokerID, current.ISR)
	}

	newLeaderEpoch := current.LeaderEpoch
	if leaderChanged {
		newLeaderEpoch++
	}

	f.state.Partitions[key] = PartitionState{
		LeaderBrokerID: cmd.LeaderBrokerID,
		LeaderEpoch:    newLeaderEpoch,
		PartitionEpoch: current.PartitionEpoch + 1,
		Replicas:       append([]int32(nil), cmd.Replicas...),
		ISR:            append([]int32(nil), cmd.ISR...),
	}
	return nil
}

func containsInt32(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// validatePartitionSpec validates a partition assignment.
func validatePartitionSpec(brokers map[int32]BrokerInfo, leaderBrokerID int32, replicas, isr []int32) error {
	if len(replicas) == 0 {
		return fmt.Errorf("replicas must not be empty")
	}

	replicaSet := make(map[int32]bool, len(replicas))
	for _, r := range replicas {
		if replicaSet[r] {
			return fmt.Errorf("duplicate replica %d", r)
		}
		replicaSet[r] = true
		if _, ok := brokers[r]; !ok {
			return fmt.Errorf("replica references unknown broker %d", r)
		}
	}

	if !replicaSet[leaderBrokerID] {
		return fmt.Errorf("leader %d is not in replicas %v", leaderBrokerID, replicas)
	}

	isrSet := make(map[int32]bool, len(isr))
	for _, m := range isr {
		if isrSet[m] {
			return fmt.Errorf("duplicate ISR member %d", m)
		}
		isrSet[m] = true
		if !replicaSet[m] {
			return fmt.Errorf("ISR member %d is not in replicas %v", m, replicas)
		}
		if info, ok := brokers[m]; !ok || info.Fenced {
			return fmt.Errorf("ISR member %d is fenced or unknown", m)
		}
	}

	if !isrSet[leaderBrokerID] {
		return fmt.Errorf("leader %d must be in the resulting ISR %v", leaderBrokerID, isr)
	}
	if info, ok := brokers[leaderBrokerID]; !ok || info.Fenced {
		return fmt.Errorf("leader %d is fenced or unknown", leaderBrokerID)
	}

	return nil
}

func (f *ControllerFSM) GetBroker(id int32) (BrokerInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, ok := f.state.Brokers[id]
	return b, ok
}

// GetPartition returns a defensive copy.
func (f *ControllerFSM) GetPartition(topic string, partition int32) (PartitionState, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.state.Partitions[TopicPartitionKey{Topic: topic, Partition: partition}]
	if !ok {
		return PartitionState{}, false
	}
	cp := p
	cp.Replicas = append([]int32(nil), p.Replicas...)
	cp.ISR = append([]int32(nil), p.ISR...)
	return cp, true
}

// CurrentSnapshotVersion is the current snapshot format.
const CurrentSnapshotVersion = 1

type snapshotBroker struct {
	ID            int32  `json:"id"`
	IncarnationID string `json:"incarnation_id"`
	BrokerEpoch   uint64 `json:"broker_epoch"`
	Fenced        bool   `json:"fenced"`
}

type snapshotTopic struct {
	Name string `json:"name"`
}

type snapshotPartition struct {
	Topic          string  `json:"topic"`
	Partition      int32   `json:"partition"`
	LeaderBrokerID int32   `json:"leader_broker_id"`
	LeaderEpoch    int32   `json:"leader_epoch"`
	PartitionEpoch int32   `json:"partition_epoch"`
	Replicas       []int32 `json:"replicas"`
	ISR            []int32 `json:"isr"`
}

type snapshotData struct {
	Version    int                 `json:"version"`
	Brokers    []snapshotBroker    `json:"brokers"`
	Topics     []snapshotTopic     `json:"topics"`
	Partitions []snapshotPartition `json:"partitions"`
}

type controllerSnapshot struct {
	state controllerState
}

var _ raft.FSMSnapshot = (*controllerSnapshot)(nil)

// Snapshot captures an immutable copy. Encoding is deferred to Persist.
func (f *ControllerFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &controllerSnapshot{state: f.state.deepCopy()}, nil
}

func cancelSnapshot(sink raft.SnapshotSink, cause error) error {
	if err := sink.Cancel(); err != nil {
		return errors.Join(cause, fmt.Errorf("controller snapshot: cancel: %w", err))
	}
	return cause
}

// Persist writes the captured state to a snapshot sink.
func (s *controllerSnapshot) Persist(sink raft.SnapshotSink) error {
	data := toSnapshotData(s.state)

	encoded, err := json.Marshal(data)
	if err != nil {
		return cancelSnapshot(sink, fmt.Errorf("controller snapshot: encode: %w", err))
	}

	n, err := sink.Write(encoded)
	if err != nil {
		return cancelSnapshot(sink, fmt.Errorf("controller snapshot: write: %w", err))
	}
	if n != len(encoded) {
		return cancelSnapshot(sink, fmt.Errorf("controller snapshot: %w", io.ErrShortWrite))
	}

	if err := sink.Close(); err != nil {
		return cancelSnapshot(sink, fmt.Errorf("controller snapshot: close: %w", err))
	}
	return nil
}

func (s *controllerSnapshot) Release() {}

// Restore validates and replaces the current state.
func (f *ControllerFSM) Restore(snapshot io.ReadCloser) error {
	raw, readErr := io.ReadAll(snapshot)
	closeErr := snapshot.Close()
	if readErr != nil || closeErr != nil {
		return errors.Join(
			wrapError("controller restore: read", readErr),
			wrapError("controller restore: close", closeErr),
		)
	}

	var data snapshotData
	if err := json.Unmarshal(raw, &data); err != nil {
		return fmt.Errorf("controller restore: decode: %w", err)
	}

	newState, err := fromSnapshotData(data)
	if err != nil {
		return fmt.Errorf("controller restore: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = newState
	return nil
}

func wrapError(message string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

func toSnapshotData(s controllerState) snapshotData {
	brokers := make([]snapshotBroker, 0, len(s.Brokers))
	for _, b := range s.Brokers {
		brokers = append(brokers, snapshotBroker{
			ID: b.ID, IncarnationID: b.IncarnationID, BrokerEpoch: b.BrokerEpoch, Fenced: b.Fenced,
		})
	}
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })

	topics := make([]snapshotTopic, 0, len(s.Topics))
	for name := range s.Topics {
		topics = append(topics, snapshotTopic{Name: name})
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })

	partitions := make([]snapshotPartition, 0, len(s.Partitions))
	for key, p := range s.Partitions {
		partitions = append(partitions, snapshotPartition{
			Topic: key.Topic, Partition: key.Partition,
			LeaderBrokerID: p.LeaderBrokerID, LeaderEpoch: p.LeaderEpoch, PartitionEpoch: p.PartitionEpoch,
			Replicas: append([]int32(nil), p.Replicas...), ISR: append([]int32(nil), p.ISR...),
		})
	}
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})

	return snapshotData{Version: CurrentSnapshotVersion, Brokers: brokers, Topics: topics, Partitions: partitions}
}

func fromSnapshotData(data snapshotData) (controllerState, error) {
	if data.Version != CurrentSnapshotVersion {
		return controllerState{}, fmt.Errorf("unsupported snapshot version %d", data.Version)
	}

	out := newControllerState()

	for _, b := range data.Brokers {
		if b.ID < 0 {
			return controllerState{}, fmt.Errorf("invalid broker id %d", b.ID)
		}
		if b.IncarnationID == "" {
			return controllerState{}, fmt.Errorf("broker %d: incarnation id required", b.ID)
		}
		if b.BrokerEpoch == 0 {
			return controllerState{}, fmt.Errorf("broker %d: invalid broker epoch 0", b.ID)
		}
		if _, exists := out.Brokers[b.ID]; exists {
			return controllerState{}, fmt.Errorf("duplicate broker %d", b.ID)
		}
		out.Brokers[b.ID] = BrokerInfo{ID: b.ID, IncarnationID: b.IncarnationID, BrokerEpoch: b.BrokerEpoch, Fenced: b.Fenced}
	}

	for _, t := range data.Topics {
		if t.Name == "" {
			return controllerState{}, fmt.Errorf("empty topic name in snapshot")
		}
		if _, exists := out.Topics[t.Name]; exists {
			return controllerState{}, fmt.Errorf("duplicate topic %q", t.Name)
		}
		out.Topics[t.Name] = TopicState{Name: t.Name}
	}

	for _, p := range data.Partitions {
		if p.Partition < 0 {
			return controllerState{}, fmt.Errorf("invalid partition index %d", p.Partition)
		}
		if p.Topic == "" {
			return controllerState{}, fmt.Errorf("empty topic name for partition %d", p.Partition)
		}
		if _, exists := out.Topics[p.Topic]; !exists {
			return controllerState{}, fmt.Errorf("partition %s/%d references unknown topic", p.Topic, p.Partition)
		}
		if p.LeaderEpoch < 0 || p.PartitionEpoch < 0 {
			return controllerState{}, fmt.Errorf("partition %s/%d: negative epoch", p.Topic, p.Partition)
		}
		key := TopicPartitionKey{Topic: p.Topic, Partition: p.Partition}
		if _, exists := out.Partitions[key]; exists {
			return controllerState{}, fmt.Errorf("duplicate partition %s/%d", p.Topic, p.Partition)
		}
		if err := validatePartitionSpec(out.Brokers, p.LeaderBrokerID, p.Replicas, p.ISR); err != nil {
			return controllerState{}, fmt.Errorf("partition %s/%d: %w", p.Topic, p.Partition, err)
		}
		out.Partitions[key] = PartitionState{
			LeaderBrokerID: p.LeaderBrokerID, LeaderEpoch: p.LeaderEpoch, PartitionEpoch: p.PartitionEpoch,
			Replicas: append([]int32(nil), p.Replicas...), ISR: append([]int32(nil), p.ISR...),
		}
	}

	partitionCount := make(map[string]int32, len(out.Topics))
	for key := range out.Partitions {
		partitionCount[key.Topic]++
	}
	for topic := range out.Topics {
		count := partitionCount[topic]
		if count == 0 {
			return controllerState{}, fmt.Errorf("topic %q has no partitions", topic)
		}
		for partition := int32(0); partition < count; partition++ {
			if _, ok := out.Partitions[TopicPartitionKey{Topic: topic, Partition: partition}]; !ok {
				return controllerState{}, fmt.Errorf("topic %q partitions are not contiguous from 0", topic)
			}
		}
	}

	return out, nil
}
