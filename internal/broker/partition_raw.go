package broker

import (
	"container/heap"
	"context"
	"fmt"
)

type FetchRawResult struct {
	// nil when there is no data on requested offset **
	Records []byte

	HighWatermark  uint64
	LogStartOffset uint64
	FetchedUpTo    uint64
}

func (p *Partition) AppendRaw(ctx context.Context, data []byte, acks Acks) (AppendResult, error) {
	if !p.isLeader.Load() {
		return AppendResult{}, ErrNotLeaderOrFollower
	}
	select {
	case <-p.closed:
		return AppendResult{}, ErrPartitionClosed
	default:
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if len(data) < 27 {
		return AppendResult{}, fmt.Errorf("AppendRaw: batch too short (%d bytes)", len(data))
	}

	var (
		firstOffset uint64
		requiredHW  uint64
		entry       *purgatoryEntry
		notify      chan struct{}
	)

	p.appendMu.Lock()

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

	p.isrMu.RLock()
	epoch := p.leaderEpoch
	p.isrMu.RUnlock()

	var lod uint32
	var err error

	firstOffset, lod, err = p.log.AppendRaw(data, int32(epoch))
	if err != nil {
		p.appendMu.Unlock()
		return AppendResult{}, fmt.Errorf("AppendRaw: log: %w", err)
	}

	requiredHW = firstOffset + uint64(lod) + 1

	if acks == AcksAll {
		notify = make(chan struct{})
		entry = &purgatoryEntry{requiredHW: requiredHW, notify: notify}
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

	return AppendResult{}, fmt.Errorf("AppendRaw: unknown acks value: %d", acks)
}

// FetchRaw is the network read path. It returns raw on-disk RecordBatch bytes
// starting at fetchOffset. No decode/re-encode occurs.
//
// maxBytes controls how many bytes are returned (minOneMessage semantics:
// at least one full batch is always returned even if it exceeds maxBytes).
//
// replicaID = -1 for consumers, broker ID for followers.
func (p *Partition) FetchRaw(fetchOffset uint64, replicaID int32, maxBytes int32) (FetchRawResult, error) {
	select {
	case <-p.closed:
		return FetchRawResult{}, ErrPartitionClosed
	default:
	}
	if !p.isLeader.Load() {
		return FetchRawResult{}, ErrNotLeaderOrFollower
	}

	isFollower := replicaID >= 0
	hw := p.highWatermark.Load()

	if !isFollower && fetchOffset >= hw {
		return FetchRawResult{HighWatermark: hw, LogStartOffset: p.log.OldestOffset()}, nil
	}

	// Followers fetch up to LEO; update their tracked position.
	if isFollower && fetchOffset >= p.log.NextOffset() {
		p.updateFollowerLEO(replicaID, fetchOffset)
		return FetchRawResult{HighWatermark: hw, LogStartOffset: p.log.OldestOffset()}, nil
	}

	raw, fetchedUpTo, err := p.log.ReadRaw(fetchOffset, maxBytes)
	if err != nil {
		return FetchRawResult{}, fmt.Errorf("FetchRaw: log read at offset %d: %w", fetchOffset, err)
	}

	if isFollower {
		p.updateFollowerLEO(replicaID, fetchedUpTo)
	}

	return FetchRawResult{
		Records:        raw,
		HighWatermark:  hw,
		LogStartOffset: p.log.OldestOffset(),
		FetchedUpTo:    fetchedUpTo,
	}, nil
}
