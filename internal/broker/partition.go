package broker

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

type Acks int16

const (
	AcksNone   Acks = 0
	AcksLeader Acks = 1
	AcksAll    Acks = -1
)

// let client decide what to do with these
type ErrorCode int16

const (
	ErrNone                        ErrorCode = 0
	ErrOffsetOutOfRange            ErrorCode = 1
	ErrCorruptMessage              ErrorCode = 2
	ErrUnknownTopicOrPartition     ErrorCode = 3
	ErrNotLeaderOrFollower         ErrorCode = 6
	ErrRequestTimedOut             ErrorCode = 7
	ErrMessageTooLarge             ErrorCode = 10
	ErrInvalidRequiredAcks         ErrorCode = 21
	ErrUnsupportedVersion          ErrorCode = 35
	ErrUnsupportedForMessageFormat ErrorCode = 43
	ErrStorageError                ErrorCode = 56
)

func (e ErrorCode) Error() string {
	switch e {
	case ErrNone:
		return "none"
	case ErrOffsetOutOfRange:
		return "offset out of range"
	case ErrCorruptMessage:
		return "corrupt message"
	case ErrUnknownTopicOrPartition:
		return "unknown topic or partition"
	case ErrNotLeaderOrFollower:
		return "not leader or follower"
	case ErrRequestTimedOut:
		return "request timed out"
	case ErrMessageTooLarge:
		return "message too large"
	case ErrInvalidRequiredAcks:
		return "invalid required acks"
	case ErrUnsupportedForMessageFormat:
		return "unsupported for message format"
	case ErrStorageError:
		return "storage error"
	default:
		return fmt.Sprintf("error code %d", int16(e))
	}
}

var ErrPartitionClosed = errors.New("partition closed")

type AppendResult struct {
	FirstOffset uint64
	Done        <-chan struct{}
	ErrFn       func() *ErrorCode
}

type FetchResult struct {
	Batch         *protocol.Batch
	HighWatermark uint64
}

type ISREntry struct {
	BrokerID int32
	LEO      uint64
}

type purgatoryEntry struct {
	requiredHW uint64
	notify     chan struct{}
	err        *ErrorCode
	index      int // heap pos - will probably be removed later (the whole heap structure)
}

// purgatoryHeap is a min heap of *purgatoryEntry ordered by requiredHW
type purgatoryHeap []*purgatoryEntry

// some are unused for now but nice to have :)
func (h purgatoryHeap) Len() int           { return len(h) }
func (h purgatoryHeap) Less(i, j int) bool { return h[i].requiredHW < h[j].requiredHW }
func (h purgatoryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *purgatoryHeap) Push(x any) {
	e := x.(*purgatoryEntry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *purgatoryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

type Partition struct {
	topicName     string
	partitionID   int32
	localBrokerID int32

	log *log.Log

	highWatermark atomic.Uint64
	isLeader      atomic.Bool
	leaderEpoch   uint32
	isr           []ISREntry // nil on followers only leader keeps track
	isrMu         sync.RWMutex
	appendMu      sync.Mutex

	purgatory   purgatoryHeap
	purgatoryMu sync.Mutex
	notifyMu sync.RWMutex
	notifyCh chan struct{}

	closed    chan struct{}
	closeOnce sync.Once
}

func NewPartition(topicName string, partitionID int32, localBrokerID int32, l *log.Log) *Partition {
	p := &Partition{
		topicName:     topicName,
		partitionID:   partitionID,
		localBrokerID: localBrokerID,
		log:           l,
		closed:        make(chan struct{}),
		notifyCh: make(chan struct{}),
	}
	heap.Init(&p.purgatory)
	p.highWatermark.Store(l.OldestOffset())
	return p
}

// WARNING MakeLeader is for leadership transitions, not in place ISR updates.
func (p *Partition) MakeLeader(epoch uint32, initialISR []int32, initialHW uint64) {
	defer p.notifyWaiters()
	p.appendMu.Lock()

	p.isrMu.Lock()
	p.leaderEpoch = epoch
	p.isr = make([]ISREntry, len(initialISR))
	for i, id := range initialISR {
		p.isr[i] = ISREntry{BrokerID: id, LEO: 0}
	}
	p.isrMu.Unlock()

	p.highWatermark.Store(initialHW)
	p.isLeader.Store(true)

	p.appendMu.Unlock()
}

func (p *Partition) MakeFollower(epoch uint32, _ int32) {
	defer p.notifyWaiters()
	p.appendMu.Lock()

	p.isLeader.Store(false)

	p.isrMu.Lock()
	p.leaderEpoch = epoch
	p.isr = nil
	p.isrMu.Unlock()

	p.appendMu.Unlock()
	notLeader := ErrNotLeaderOrFollower
	p.failPurgatory(&notLeader) // clean purgatory of now follower (old leader)
}

// TODO implement hierarchical timing wheel after benchmarking <- on ctx cancel, read confluent write up on it
func (p *Partition) Append(ctx context.Context, batch *protocol.Batch, acks Acks) (AppendResult, error) {

	if !p.isLeader.Load() {
		return AppendResult{}, ErrNotLeaderOrFollower
	}
	select {
	case <-p.closed:
		return AppendResult{}, ErrPartitionClosed
	default:
	}

	var (
		firstOffset uint64
		requiredHW  uint64
		entry       *purgatoryEntry
		notify      chan struct{}
	)

	if ctx == nil {
		ctx = context.Background()
	}

	p.appendMu.Lock()

	// rechecking to prevent race conditions, this could probably be better implemented?
	select {
	case <-p.closed:
		p.appendMu.Unlock()
		return AppendResult{}, ErrPartitionClosed
	default:
	}

	if !p.isLeader.Load() {
		p.appendMu.Unlock()
		return AppendResult{}, ErrNotLeaderOrFollower
	}

	var err error
	firstOffset, err = p.log.Append(batch)
	if err != nil {
		p.appendMu.Unlock()
		return AppendResult{}, fmt.Errorf("log append: %w", err)
	}

	requiredHW = firstOffset + uint64(len(batch.Messages))

	if acks == AcksAll {
		notify = make(chan struct{})
		entry = &purgatoryEntry{
			requiredHW: requiredHW,
			notify:     notify,
		}

		p.purgatoryMu.Lock()
		heap.Push(&p.purgatory, entry)
		p.purgatoryMu.Unlock()
	}

	p.appendMu.Unlock()

	switch acks {
	case AcksNone, AcksLeader:

		p.maybeAdvanceHW()

		done := make(chan struct{})
		close(done)
		return AppendResult{
			FirstOffset: firstOffset,
			Done:        done,
			ErrFn:       func() *ErrorCode { return nil },
		}, nil

	case AcksAll:
		p.maybeAdvanceHW()

		if ctxDone := ctx.Done(); ctxDone != nil {
			go func() {
				select {
				case <-notify:

				case <-ctxDone:
					p.purgatoryMu.Lock()
					if entry.index < p.purgatory.Len() && p.purgatory[entry.index] == entry {
						heap.Remove(&p.purgatory, entry.index)
						timeoutErr := ErrRequestTimedOut
						entry.err = &timeoutErr
						close(notify)
					}
					p.purgatoryMu.Unlock()
				}
			}()
		}

		return AppendResult{
			FirstOffset: firstOffset,
			Done:        notify,
			ErrFn:       func() *ErrorCode { return entry.err },
		}, nil
	}

	return AppendResult{}, fmt.Errorf("unknown acks value: %d", acks)
}

// currently returns the whole batch (even past hw) but clients should filter with the returned hw (will investigate later)
// replicas can read past hw
func (p *Partition) Fetch(fetchOffset uint64, followerBrokerID int32, isFollower bool) (FetchResult, error) {
	select {
	case <-p.closed:
		return FetchResult{}, ErrPartitionClosed
	default:
	}
	if !p.isLeader.Load() {
		return FetchResult{}, ErrNotLeaderOrFollower
	}

	hw := p.highWatermark.Load()

	if !isFollower && fetchOffset >= hw {
		return FetchResult{HighWatermark: hw}, nil
	}

	if isFollower && fetchOffset >= p.log.NextOffset() {
		p.updateFollowerLEO(followerBrokerID, fetchOffset)
		return FetchResult{HighWatermark: hw}, nil
	}

	batch, err := p.log.Read(fetchOffset)
	if err != nil {
		return FetchResult{}, fmt.Errorf("log read at offset %d: %w", fetchOffset, err)
	}

	if isFollower {
		fetchedUpTo := batch.FirstOffset + uint64(batch.LastOffsetDelta) + 1
		p.updateFollowerLEO(followerBrokerID, fetchedUpTo)
	}

	return FetchResult{Batch: batch, HighWatermark: hw}, nil
}

func (p *Partition) updateFollowerLEO(brokerID int32, leo uint64) {
	p.isrMu.Lock()
	for i := range p.isr {
		if p.isr[i].BrokerID == brokerID {
			p.isr[i].LEO = leo
			break
		}
	}
	p.isrMu.Unlock()
	p.maybeAdvanceHW()
}

func (p *Partition) maybeAdvanceHW() {
	if !p.isLeader.Load() {
		return
	}

	p.isrMu.RLock()
	isr := p.isr
	leaderLEO := p.log.NextOffset()

	newHW := leaderLEO
	for _, entry := range isr {
		if entry.BrokerID == p.localBrokerID {
			continue
		}
		if entry.LEO < newHW {
			newHW = entry.LEO
		}
	}
	p.isrMu.RUnlock()

	if len(isr) == 0 {
		return
	}

	for {
		current := p.highWatermark.Load()
		if newHW <= current {
			p.drainPurgatory(current)
			return
		}
		if p.highWatermark.CompareAndSwap(current, newHW) {
			break
		}
	}

	p.drainPurgatory(newHW)
	p.notifyWaiters()
}

// drain purgatory up to and including hw
func (p *Partition) drainPurgatory(newHW uint64) {
	p.purgatoryMu.Lock()
	defer p.purgatoryMu.Unlock()

	for p.purgatory.Len() > 0 {
		top := p.purgatory[0]
		if top.requiredHW > newHW {
			break
		}
		heap.Pop(&p.purgatory)
		close(top.notify)
	}
}

func (p *Partition) failPurgatory(err *ErrorCode) {
	p.purgatoryMu.Lock()
	defer p.purgatoryMu.Unlock()

	for p.purgatory.Len() > 0 {
		entry := heap.Pop(&p.purgatory).(*purgatoryEntry)
		entry.err = err
		close(entry.notify)
	}
}

func (p *Partition) Close() error {
	var logErr error

	p.closeOnce.Do(func() {
		close(p.closed)
		defer p.notifyWaiters()
		p.appendMu.Lock()
		defer p.appendMu.Unlock()

		storageErr := ErrStorageError
		p.failPurgatory(&storageErr)

		logErr = p.log.Close()
	})

	return logErr
}

func (p *Partition) HighWatermark() uint64 { return p.highWatermark.Load() }
func (p *Partition) LEO() uint64           { return p.log.NextOffset() }
func (p *Partition) IsLeader() bool        { return p.isLeader.Load() }

func (p *Partition) NotifyChan() <-chan struct{} {
	p.notifyMu.RLock()
	defer p.notifyMu.RUnlock()
	return p.notifyCh
}

func (p *Partition) notifyWaiters() {
	p.notifyMu.Lock()
	close(p.notifyCh)
	p.notifyCh = make(chan struct{})
	p.notifyMu.Unlock()
}

func (p *Partition) LeaderEpoch() uint32 {
	p.isrMu.RLock()
	defer p.isrMu.RUnlock()
	return p.leaderEpoch
}

func (p *Partition) ISRSnapshot() []int32 {
	p.isrMu.RLock()
	defer p.isrMu.RUnlock()
	ids := make([]int32, len(p.isr))
	for i, e := range p.isr {
		ids[i] = e.BrokerID
	}
	return ids
}
