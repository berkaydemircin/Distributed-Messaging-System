package broker

import (
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/controller"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

type topicPartitionKey struct {
	topic     string
	partition int32
}

// FetchManager groups follower partitions by source leader.
type FetchManager struct {
	brokerID   int32
	topics     *TopicManager
	controller Controller
	logger     *slog.Logger

	dialTimeout    time.Duration
	requestTimeout time.Duration
	fetchInterval  time.Duration
	syncInterval   time.Duration

	mu       sync.Mutex
	workers  map[int32]*leaderFetchWorker
	started  bool
	stopped  bool
	loopStop chan struct{}
	loopDone chan struct{}
}

func NewFetchManager(brokerID int32, topics *TopicManager, ctrl Controller, logger *slog.Logger) *FetchManager {
	return &FetchManager{
		brokerID:       brokerID,
		topics:         topics,
		controller:     ctrl,
		logger:         logger,
		dialTimeout:    5 * time.Second,
		requestTimeout: 30 * time.Second,
		fetchInterval:  200 * time.Millisecond,
		syncInterval:   time.Second,
		workers:        make(map[int32]*leaderFetchWorker),
		loopStop:       make(chan struct{}),
		loopDone:       make(chan struct{}),
	}
}

// Start begins periodic assignment reconciliation.
func (fm *FetchManager) Start() error {
	if err := fm.refresh(); err != nil {
		return err
	}

	fm.mu.Lock()
	if fm.stopped {
		fm.mu.Unlock()
		return fmt.Errorf("fetch manager: stopped")
	}
	if fm.started {
		fm.mu.Unlock()
		return nil
	}
	fm.started = true
	fm.mu.Unlock()

	go fm.run()
	return nil
}

func (fm *FetchManager) run() {
	defer close(fm.loopDone)
	ticker := time.NewTicker(fm.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-fm.loopStop:
			return
		case <-ticker.C:
			if err := fm.refresh(); err != nil {
				fm.logger.Warn("fetch manager: refresh failed", "err", err)
			}
		}
	}
}

func (fm *FetchManager) refresh() error {
	if err := fm.topics.Reconcile(fm.controller, fm.brokerID); err != nil {
		return fmt.Errorf("fetch manager: reconcile: %w", err)
	}
	return fm.Reassign()
}

// Reassign updates workers from the current controller assignment.
func (fm *FetchManager) Reassign() error {
	assignments := fm.controller.PartitionsForBroker(fm.brokerID)

	byLeader := make(map[int32][]controller.PartitionMetadata)
	for _, pm := range assignments {
		if pm.LeaderBrokerID == fm.brokerID {
			continue // we lead this partition ourselves; nothing to fetch
		}
		byLeader[pm.LeaderBrokerID] = append(byLeader[pm.LeaderBrokerID], pm)
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.stopped {
		return fmt.Errorf("fetch manager: stopped")
	}

	for leaderID, w := range fm.workers {
		if _, stillNeeded := byLeader[leaderID]; !stillNeeded {
			w.stop()
			delete(fm.workers, leaderID)
		}
	}

	for leaderID, parts := range byLeader {
		leaderInfo, found := fm.controller.Broker(leaderID)
		ep, endpointFound := plaintextEndpoint(leaderInfo.Endpoints)
		if !found || leaderInfo.Fenced || !endpointFound {
			fm.logger.Warn("fetch manager: leader is unavailable, deferring", "leader", leaderID)
			if w, ok := fm.workers[leaderID]; ok {
				w.stop()
				delete(fm.workers, leaderID)
			}
			continue
		}
		addr := net.JoinHostPort(ep.Host, strconv.Itoa(int(ep.Port)))

		w, ok := fm.workers[leaderID]
		if ok && w.addr != addr {
			w.stop()
			delete(fm.workers, leaderID)
			ok = false
		}
		if !ok {
			w = newLeaderFetchWorker(fm.brokerID, leaderID, addr, fm.dialTimeout, fm.requestTimeout, fm.fetchInterval, fm.logger)
			fm.workers[leaderID] = w
			w.start()
		}
		w.setPartitions(fm.resolvePartitions(parts))
	}
	return nil
}

func plaintextEndpoint(endpoints []controller.BrokerEndpoint) (controller.BrokerEndpoint, bool) {
	for _, endpoint := range endpoints {
		if endpoint.Name == "PLAINTEXT" && endpoint.SecurityProtocol == controller.SecurityProtocolPlaintext {
			return endpoint, true
		}
	}
	return controller.BrokerEndpoint{}, false
}

type assignedPartition struct {
	partition   *Partition
	leaderEpoch int32
}

func (fm *FetchManager) resolvePartitions(assignments []controller.PartitionMetadata) map[topicPartitionKey]assignedPartition {
	out := make(map[topicPartitionKey]assignedPartition, len(assignments))
	for _, pm := range assignments {
		p, errCode := fm.topics.GetPartition(pm.Topic, pm.Partition)
		if errCode != ErrNone {
			fm.logger.Warn("fetch manager: no local partition for assignment yet", "topic", pm.Topic, "partition", pm.Partition)
			continue
		}
		out[topicPartitionKey{topic: pm.Topic, partition: pm.Partition}] = assignedPartition{
			partition: p, leaderEpoch: pm.LeaderEpoch,
		}
	}
	return out
}

// Stop stops every worker and waits for them to fully exit.
func (fm *FetchManager) Stop() {
	fm.mu.Lock()
	if fm.stopped {
		fm.mu.Unlock()
		return
	}
	fm.stopped = true
	if fm.started {
		close(fm.loopStop)
	}
	workers := fm.workers
	fm.workers = nil
	started := fm.started
	fm.mu.Unlock()

	if started {
		<-fm.loopDone
	}
	for _, w := range workers {
		w.stop()
	}
}

type leaderFetchWorker struct {
	localBrokerID  int32
	leaderBrokerID int32
	addr           string
	client         *ReplicaClient
	requestTimeout time.Duration
	fetchInterval  time.Duration
	logger         *slog.Logger

	mu         sync.Mutex
	partitions map[topicPartitionKey]assignedPartition
	reconciled map[topicPartitionKey]int32

	stopCh chan struct{}
	doneCh chan struct{}
}

func newLeaderFetchWorker(localBrokerID, leaderBrokerID int32, addr string, dialTimeout, requestTimeout, fetchInterval time.Duration, logger *slog.Logger) *leaderFetchWorker {
	return &leaderFetchWorker{
		localBrokerID:  localBrokerID,
		leaderBrokerID: leaderBrokerID,
		addr:           addr,
		client:         NewReplicaClient(addr, dialTimeout, requestTimeout),
		requestTimeout: requestTimeout,
		fetchInterval:  fetchInterval,
		logger:         logger,
		partitions:     make(map[topicPartitionKey]assignedPartition),
		reconciled:     make(map[topicPartitionKey]int32),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
}

func (w *leaderFetchWorker) setPartitions(parts map[topicPartitionKey]assignedPartition) {
	w.mu.Lock()
	defer w.mu.Unlock()
	nextReconciled := make(map[topicPartitionKey]int32, len(parts))
	for key, assignment := range parts {
		if epoch, ok := w.reconciled[key]; ok && epoch == assignment.leaderEpoch {
			nextReconciled[key] = epoch
		}
	}
	w.partitions = parts
	w.reconciled = nextReconciled
}

func (w *leaderFetchWorker) start() {
	go w.run()
}

func (w *leaderFetchWorker) stop() {
	close(w.stopCh)
	w.client.Shutdown()
	<-w.doneCh
}

// reconcilePartition truncates a follower to its last common epoch.
func (w *leaderFetchWorker) reconcilePartition(key topicPartitionKey, p *Partition, currentLeaderEpoch int32) error {
	followerEpoch, ok := p.log.LatestLeaderEpoch()
	if !ok {
		return nil
	}

	for attempts := 0; attempts < 32; attempts++ {
		resp, err := w.client.OffsetsForLeaderEpoch(&protocol.OffsetsForLeaderEpochRequest{
			ReplicaID: w.localBrokerID,
			Topics: []protocol.OffsetsForLeaderEpochRequestTopic{
				{Name: key.topic, Partitions: []protocol.OffsetsForLeaderEpochRequestPartition{
					{Index: key.partition, CurrentLeaderEpoch: currentLeaderEpoch, LeaderEpoch: followerEpoch},
				}},
			},
		}, 3)
		if err != nil {
			return fmt.Errorf("OffsetsForLeaderEpoch: %w", err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			return fmt.Errorf("OffsetsForLeaderEpoch: incomplete response")
		}
		rp := resp.Topics[0].Partitions[0]
		if rp.ErrorCode != 0 {
			return fmt.Errorf("OffsetsForLeaderEpoch: error code %d", rp.ErrorCode)
		}
		if rp.LeaderEpoch < 0 || rp.EndOffset < 0 {
			return fmt.Errorf("OffsetsForLeaderEpoch: undefined epoch or offset")
		}

		leaderEnd := uint64(rp.EndOffset)
		truncationOffset := leaderEnd
		localEnd, found := p.log.EndOffsetForLeaderEpoch(rp.LeaderEpoch)
		if found && localEnd.EndOffset < truncationOffset {
			truncationOffset = localEnd.EndOffset
		}
		if leo := p.LEO(); leo < truncationOffset {
			truncationOffset = leo
		}
		if truncationOffset < p.LEO() {
			if err := p.TruncateTo(truncationOffset); err != nil {
				return fmt.Errorf("TruncateTo(%d): %w", truncationOffset, err)
			}
		}

		if !found || localEnd.Epoch == rp.LeaderEpoch {
			return nil
		}
		followerEpoch = localEnd.Epoch
	}
	return fmt.Errorf("OffsetsForLeaderEpoch: reconciliation did not converge")
}

func (w *leaderFetchWorker) run() {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.fetchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.fetchOnce()
		}
	}
}

// fetchOnce batches the worker's ready partitions into one request.
func (w *leaderFetchWorker) fetchOnce() {
	w.mu.Lock()
	if len(w.partitions) == 0 {
		w.mu.Unlock()
		return
	}
	toReconcile := make(map[topicPartitionKey]assignedPartition)
	for key, assignment := range w.partitions {
		if epoch, ok := w.reconciled[key]; !ok || epoch != assignment.leaderEpoch {
			toReconcile[key] = assignment
		}
	}
	w.mu.Unlock()

	for key, assignment := range toReconcile {
		if err := w.reconcilePartition(key, assignment.partition, assignment.leaderEpoch); err != nil {
			w.logger.Warn("fetch manager: reconciliation failed, will retry next round",
				"topic", key.topic, "partition", key.partition, "leader", w.leaderBrokerID, "err", err)
			continue // leave unreconciled; retried on the next round
		}
		w.mu.Lock()
		current, exists := w.partitions[key]
		if exists && current.partition == assignment.partition && current.leaderEpoch == assignment.leaderEpoch {
			w.reconciled[key] = assignment.leaderEpoch
		}
		w.mu.Unlock()
	}

	type requestState struct {
		partition   *Partition
		fetchOffset uint64
		leaderEpoch int32
	}
	requestStates := make(map[topicPartitionKey]requestState)

	w.mu.Lock()
	byTopic := make(map[string][]protocol.FetchRequestPartition)
	for key, assignment := range w.partitions {
		if epoch, ok := w.reconciled[key]; !ok || epoch != assignment.leaderEpoch {
			continue
		}
		p := assignment.partition
		fetchOffset := p.LEO()
		requestStates[key] = requestState{
			partition: p, fetchOffset: fetchOffset, leaderEpoch: assignment.leaderEpoch,
		}
		byTopic[key.topic] = append(byTopic[key.topic], protocol.FetchRequestPartition{
			Index:              key.partition,
			CurrentLeaderEpoch: assignment.leaderEpoch,
			FetchOffset:        int64(fetchOffset),
			LogStartOffset:     int64(p.log.OldestOffset()),
			MaxBytes:           1 << 20,
		})
	}
	w.mu.Unlock()
	if len(requestStates) == 0 {
		return
	}

	req := &protocol.FetchRequest{
		ReplicaID: w.localBrokerID,
		MaxWaitMs: 500,
		MinBytes:  1,
		MaxBytes:  4 << 20,
	}
	for topic, parts := range byTopic {
		req.Topics = append(req.Topics, protocol.FetchRequestTopic{Name: topic, Partitions: parts})
	}

	resp, err := w.client.Fetch(req, 9)
	if err != nil {
		w.logger.Warn("fetch manager: fetch failed", "leader", w.leaderBrokerID, "err", err)
		return
	}
	if resp.ErrorCode != 0 {
		w.logger.Warn("fetch manager: top-level fetch error", "leader", w.leaderBrokerID, "code", resp.ErrorCode)
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, t := range resp.Topics {
		for _, fp := range t.Partitions {
			key := topicPartitionKey{topic: t.Name, partition: fp.Index}
			state, requested := requestStates[key]
			current, assigned := w.partitions[key]
			if !requested || !assigned || current.partition != state.partition ||
				current.leaderEpoch != state.leaderEpoch || state.partition.LeaderEpoch() != state.leaderEpoch ||
				state.partition.LEO() != state.fetchOffset {
				continue
			}
			if fp.ErrorCode != 0 {
				w.logger.Warn("fetch manager: partition error", "topic", t.Name, "partition", fp.Index, "code", fp.ErrorCode)
				continue
			}
			if fp.HighWatermark < 0 {
				w.logger.Warn("fetch manager: invalid high watermark", "topic", t.Name, "partition", fp.Index, "highWatermark", fp.HighWatermark)
				continue
			}
			if err := state.partition.AppendFromLeader(fp.Records, state.leaderEpoch, uint64(fp.HighWatermark)); err != nil {
				w.logger.Warn("fetch manager: AppendFromLeader failed", "topic", t.Name, "partition", fp.Index, "err", err)
			}
		}
	}
}
