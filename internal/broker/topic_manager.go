package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/controller"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
)

/*
 * TODO change fmt.printf to slog
 */

// partitions are immutable after creation for now, ordering is also local to partitions. (no global ordering)
type Topic struct {
	name       string
	partitions []*Partition
}

// sync.Map could also be used but i chose type safety, the performance of this path is not critical anyways
type TopicManager struct {
	dataDir       string
	localBrokerID int32
	logConfig     log.LogConfig

	mu     sync.RWMutex
	topics map[string]*Topic
}

func NewTopicManager(dataDir string, localBrokerID int32, logConfig log.LogConfig) (*TopicManager, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	tm := &TopicManager{
		dataDir:       dataDir,
		localBrokerID: localBrokerID,
		logConfig:     logConfig,
		topics:        make(map[string]*Topic),
	}
	if err := tm.recoverFromDisk(); err != nil {
		return nil, fmt.Errorf("recover topics: %w", err)
	}
	return tm, nil
}

// recoverFromDisk opens local logs without assigning partition roles.
func (tm *TopicManager) recoverFromDisk() error {
	entries, err := os.ReadDir(tm.dataDir)
	if err != nil {
		return fmt.Errorf("read data dir: %w", err)
	}

	type partitionEntry struct {
		id  int
		dir string
	}
	byTopic := make(map[string][]partitionEntry)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		idx := strings.LastIndex(name, "-")
		if idx < 0 {
			continue
		}
		topicName := name[:idx]
		if err := validateTopicName(topicName); err != nil {
			continue
		}

		partID, err := strconv.Atoi(name[idx+1:])
		if err != nil || partID < 0 {
			continue
		}
		byTopic[topicName] = append(byTopic[topicName], partitionEntry{id: partID, dir: name})
	}

	for topicName, dirs := range byTopic {
		maxID := 0
		for _, d := range dirs {
			if d.id > maxID {
				maxID = d.id
			}
		}

		// adding as a sanity check
		if maxID > 9999 {
			fmt.Printf("warn: skipping topic %q: suspiciously large partition ID %d\n",
				topicName, maxID)
			continue
		}

		partitions := make([]*Partition, maxID+1)
		for _, d := range dirs {
			partDir := filepath.Join(tm.dataDir, d.dir)
			l, err := log.NewLog(partDir, tm.logConfig)
			if err != nil {
				fmt.Printf("warn: partition %s-%d offline on recovery: %v\n",
					topicName, d.id, err)
				continue
			}
			p := NewPartition(topicName, int32(d.id), tm.localBrokerID, l)
			partitions[d.id] = p
		}

		tm.topics[topicName] = &Topic{name: topicName, partitions: partitions}
	}

	return nil
}

func (tm *TopicManager) Reconcile(ctrl Controller, brokerID int32) error {
	assignments := ctrl.PartitionsForBroker(brokerID)
	desired := make(map[topicPartitionKey]controller.PartitionMetadata, len(assignments))
	for _, pm := range assignments {
		desired[topicPartitionKey{topic: pm.Topic, partition: pm.Partition}] = pm
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Retain local data for removed replicas but stop serving
	for topic, t := range tm.topics {
		for partition, p := range t.partitions {
			if p == nil {
				continue
			}
			key := topicPartitionKey{topic: topic, partition: int32(partition)}
			if _, ok := desired[key]; !ok {
				leaderID, _, isLeader := p.Assignment()
				if isLeader || leaderID >= 0 {
					p.MakeUnassigned()
				}
			}
		}
	}

	for _, pm := range assignments {
		t, ok := tm.topics[pm.Topic]
		if !ok {
			t = &Topic{name: pm.Topic}
			tm.topics[pm.Topic] = t
		}
		if int(pm.Partition) >= len(t.partitions) {
			grown := make([]*Partition, pm.Partition+1)
			copy(grown, t.partitions)
			t.partitions = grown
		}

		p := t.partitions[pm.Partition]
		if p == nil {
			partDir := tm.partitionDir(pm.Topic, int(pm.Partition))
			l, err := log.NewLog(partDir, tm.logConfig)
			if err != nil {
				return fmt.Errorf("reconcile %s-%d: open log: %w", pm.Topic, pm.Partition, err)
			}
			p = NewPartition(pm.Topic, pm.Partition, brokerID, l)
			t.partitions[pm.Partition] = p
		}

		leaderID, epoch, isLeader := p.Assignment()
		if pm.LeaderBrokerID == brokerID {
			if isLeader && leaderID == brokerID && epoch == pm.LeaderEpoch {
				if !equalInt32s(p.ISRSnapshot(), pm.ISR) {
					if err := p.UpdateLeaderISR(pm.LeaderEpoch, pm.ISR); err != nil {
						return fmt.Errorf("reconcile %s-%d: update ISR: %w", pm.Topic, pm.Partition, err)
					}
				}
				continue
			}
			if err := p.MakeLeader(pm.LeaderEpoch, pm.ISR, p.HighWatermark()); err != nil {
				return fmt.Errorf("reconcile %s-%d: MakeLeader: %w", pm.Topic, pm.Partition, err)
			}
		} else if isLeader || leaderID != pm.LeaderBrokerID || epoch != pm.LeaderEpoch {
			p.MakeFollower(pm.LeaderEpoch, pm.LeaderBrokerID)
		}
	}
	return nil
}

func equalInt32s(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (tm *TopicManager) CreateTopic(name string, numPartitions int, initialLeader bool) error {
	if err := validateTopicName(name); err != nil {
		return err
	}
	if numPartitions <= 0 {
		return fmt.Errorf("numPartitions must be > 0, got %d", numPartitions)
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	partitions := make([]*Partition, numPartitions)
	for i := 0; i < numPartitions; i++ {
		partDir := tm.partitionDir(name, i)
		l, err := log.NewLog(partDir, tm.logConfig)
		if err != nil {
			for j := 0; j < i; j++ {
				if partitions[j] != nil {
					_ = partitions[j].Close()
				}
				_ = os.RemoveAll(tm.partitionDir(name, j))
			}
			return fmt.Errorf("create log for %s-%d: %w", name, i, err)
		}
		p := NewPartition(name, int32(i), tm.localBrokerID, l)
		if initialLeader {
			if err := p.MakeLeader(1, []int32{tm.localBrokerID}, l.NextOffset()); err != nil {
				_ = p.Close()

				for j := 0; j < i; j++ {
					if partitions[j] != nil {
						_ = partitions[j].Close()
					}
					_ = os.RemoveAll(tm.partitionDir(name, j))
				}

				_ = os.RemoveAll(tm.partitionDir(name, i))

				return fmt.Errorf(
					"make leader for %s-%d: %w",
					name,
					i,
					err,
				)
			}
		}
		partitions[i] = p
	}

	tm.topics[name] = &Topic{name: name, partitions: partitions}
	return nil
}

func (tm *TopicManager) GetPartition(topic string, partitionID int32) (*Partition, ErrorCode) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.topics[topic]

	if !ok {
		return nil, ErrUnknownTopicOrPartition
	}
	if partitionID < 0 || int(partitionID) >= len(t.partitions) {
		return nil, ErrUnknownTopicOrPartition
	}
	p := t.partitions[partitionID]
	if p == nil {
		return nil, ErrStorageError
	}
	return p, ErrNone
}

func (tm *TopicManager) DeleteTopic(name string) error {
	tm.mu.Lock()
	t, ok := tm.topics[name]
	if !ok {
		tm.mu.Unlock()
		return fmt.Errorf("unknown topic %q", name)
	}
	delete(tm.topics, name)
	tm.mu.Unlock()

	var firstErr error
	for i, p := range t.partitions {
		if p == nil {
			continue
		}
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.RemoveAll(tm.partitionDir(name, i)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (tm *TopicManager) Close() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	var firstErr error
	for _, t := range tm.topics {
		for _, p := range t.partitions {
			if p == nil {
				continue
			}
			if err := p.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	tm.topics = make(map[string]*Topic)
	return firstErr
}

// sort for determinism
func (tm *TopicManager) TopicNames() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	names := make([]string, 0, len(tm.topics))
	for name := range tm.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (tm *TopicManager) PartitionCount(topic string) (int, ErrorCode) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.topics[topic]
	if !ok {
		return 0, ErrUnknownTopicOrPartition
	}
	return len(t.partitions), ErrNone
}

func (tm *TopicManager) partitionDir(topic string, partitionID int) string {
	return filepath.Join(tm.dataDir, fmt.Sprintf("%s-%d", topic, partitionID))
}

func validateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if name == "." || name == ".." {
		return fmt.Errorf("topic name cannot be %q", name)
	}
	if len(name) > 249 {
		return fmt.Errorf("topic name too long: %d chars (max 249)", len(name))
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return fmt.Errorf("topic name %q contains invalid character %q", name, c)
		}
	}
	return nil
}
