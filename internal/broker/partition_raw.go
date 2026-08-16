package broker

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	storagelog "github.com/berkaydemircin/Distributed-Messaging-System/internal/log"
)

// TODO this feels like bad design having this all over the place, used by capHW at the bottom
const (
	recordBatchOverhead   = 61 // check internal/log for these
	batchMetaPrefixLength = 27
)

type FetchRawResult struct {
	// nil when there is no data on requested offset **
	Records []byte

	HighWatermark  uint64
	LogStartOffset uint64
	FetchedUpTo    uint64
}

type FetchRawRangeResult struct {
	Ranges         []storagelog.RawRange
	RecordsSize    int64
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

	if len(data) < recordBatchOverhead {
		return AppendResult{}, ErrCorruptMessage
	}

	if acks != AcksNone && acks != AcksLeader && acks != AcksAll {
		return AppendResult{}, ErrInvalidRequiredAcks
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
		switch {
		case errors.Is(err, storagelog.ErrCorruptBatch):
			return AppendResult{}, ErrCorruptMessage
		case errors.Is(err, storagelog.ErrUnsupportedMessageFormat):
			return AppendResult{}, ErrUnsupportedForMessageFormat
		case errors.Is(err, storagelog.ErrMessageTooLarge):
			return AppendResult{}, ErrMessageTooLarge
		}
		return AppendResult{}, fmt.Errorf("AppendRaw: log: %w", err)
	}

	requiredHW = firstOffset + uint64(lod) + 1
	p.notifyWaiters()
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

		select {
		case <-notify:
			return AppendResult{
				FirstOffset: firstOffset,
				Done:        notify,
				ErrFn:       func() *ErrorCode { return entry.err },
			}, nil
		default:
		}

		if ctxDone := ctx.Done(); ctxDone != nil {
			select {
			case <-ctxDone:
				p.purgatoryMu.Lock()
				if entry.index < p.purgatory.Len() && p.purgatory[entry.index] == entry {
					heap.Remove(&p.purgatory, entry.index)
					timeoutErr := ErrRequestTimedOut
					entry.err = &timeoutErr
					close(notify)
				}
				p.purgatoryMu.Unlock()
			default:
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
		}

		return AppendResult{
			FirstOffset: firstOffset,
			Done:        notify,
			ErrFn:       func() *ErrorCode { return entry.err },
		}, nil
	}

	return AppendResult{}, fmt.Errorf("AppendRaw: unknown acks value: %d", acks)
}

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
	leaderLEO := p.log.NextOffset()
	logStart := p.log.OldestOffset()

	if fetchOffset < logStart || fetchOffset > leaderLEO {
		return FetchRawResult{}, ErrOffsetOutOfRange
	}

	hw := p.highWatermark.Load()

	if !isFollower && fetchOffset >= hw {
		return FetchRawResult{HighWatermark: hw, LogStartOffset: logStart}, nil
	}

	if isFollower && fetchOffset == leaderLEO {
		p.updateFollowerLEO(replicaID, fetchOffset)
		hw = p.highWatermark.Load()
		return FetchRawResult{HighWatermark: hw, LogStartOffset: logStart}, nil
	}

	raw, fetchedUpTo, err := p.log.ReadRaw(fetchOffset, maxBytes)
	if err != nil {
		return FetchRawResult{}, fmt.Errorf("FetchRaw: log read at offset %d: %w", fetchOffset, err)
	}

	if isFollower {
		p.updateFollowerLEO(replicaID, fetchOffset)
		hw = p.highWatermark.Load()
	}

	if !isFollower && fetchedUpTo > hw {
		n, upto, err := capToHW(bytes.NewReader(raw), 0, int64(len(raw)), hw, fetchOffset)
		if err != nil {
			return FetchRawResult{}, fmt.Errorf("FetchRaw: capping to HW: %w", err)
		}
		if n == 0 {
			raw = nil
		} else {
			raw = raw[:n]
		}
		fetchedUpTo = upto
	}

	return FetchRawResult{
		Records:        raw,
		HighWatermark:  hw,
		LogStartOffset: logStart,
		FetchedUpTo:    fetchedUpTo,
	}, nil
}

func (p *Partition) FetchRawRanges(fetchOffset uint64, replicaID int32, maxBytes int32, allowOversizedFirstBatch bool) (FetchRawRangeResult, error) {
	select {
	case <-p.closed:
		return FetchRawRangeResult{}, ErrPartitionClosed
	default:
	}
	if !p.isLeader.Load() {
		return FetchRawRangeResult{}, ErrNotLeaderOrFollower
	}

	isFollower := replicaID >= 0
	leaderLEO := p.log.NextOffset()
	logStart := p.log.OldestOffset()

	if fetchOffset < logStart || fetchOffset > leaderLEO {
		return FetchRawRangeResult{}, ErrOffsetOutOfRange
	}

	hw := p.highWatermark.Load()

	if !isFollower && fetchOffset >= hw {

		return FetchRawRangeResult{
			HighWatermark:  hw,
			LogStartOffset: logStart,
			FetchedUpTo:    fetchOffset,
		}, nil
	}

	if isFollower && fetchOffset == leaderLEO {
		p.updateFollowerLEO(replicaID, fetchOffset)
		hw = p.highWatermark.Load()

		return FetchRawRangeResult{
			HighWatermark:  hw,
			LogStartOffset: logStart,
			FetchedUpTo:    fetchOffset,
		}, nil
	}

	raw, err := p.log.ReadRawRanges(fetchOffset, maxBytes)
	if err != nil {
		return FetchRawRangeResult{}, fmt.Errorf("FetchRawRanges: log read at offset %d: %w", fetchOffset, err)
	}

	if isFollower {
		p.updateFollowerLEO(replicaID, fetchOffset)
		hw = p.highWatermark.Load()
	}

	ranges := raw.Ranges
	recordsSize := raw.Bytes
	fetchedUpTo := raw.FetchedUpTo

	if !allowOversizedFirstBatch && recordsSize > int64(maxBytes) {
		return FetchRawRangeResult{
			HighWatermark:  hw,
			LogStartOffset: logStart,
			FetchedUpTo:    fetchOffset,
		}, nil
	}

	if !isFollower && fetchedUpTo > hw {
		capped := make([]storagelog.RawRange, 0, len(ranges))
		var size int64
		upto := fetchOffset
		for _, rg := range ranges {
			n, u, err := capToHW(rg.File, rg.Offset, rg.Length, hw, upto)
			if err != nil {
				return FetchRawRangeResult{}, fmt.Errorf("FetchRawRanges: capping to HW: %w", err)
			}
			if n > 0 {
				capped = append(capped, storagelog.RawRange{File: rg.File, Offset: rg.Offset, Length: n})
				size += n
				upto = u
			}
			if n < rg.Length {
				break
			}
		}
		ranges = capped
		recordsSize = size
		fetchedUpTo = upto
	}

	return FetchRawRangeResult{
		Ranges:         ranges,
		RecordsSize:    recordsSize,
		HighWatermark:  hw,
		LogStartOffset: logStart,
		FetchedUpTo:    fetchedUpTo,
	}, nil
}

func capToHW(r io.ReaderAt, pos, length int64, maxOffset, fallback uint64) (cappedLength int64, exclusiveEnd uint64, err error) {
	exclusiveEnd = fallback
	var consumed int64
	var hdr [batchMetaPrefixLength]byte

	for consumed < length {
		if length-consumed < batchMetaPrefixLength {
			return 0, fallback, fmt.Errorf(
				"capToHW: %d trailing bytes at offset %d don't form a complete batch header",
				length-consumed, pos+consumed)
		}
		if _, err := r.ReadAt(hdr[:], pos+consumed); err != nil {
			return 0, fallback, fmt.Errorf("capToHW: read at %d: %w", pos+consumed, err)
		}

		baseOffset := binary.BigEndian.Uint64(hdr[0:8])
		batchLength := binary.BigEndian.Uint32(hdr[8:12])
		lastOffsetDelta := binary.BigEndian.Uint32(hdr[23:27])

		totalSize := int64(12) + int64(batchLength)
		if totalSize < recordBatchOverhead {
			return 0, fallback, fmt.Errorf(
				"capToHW: batch at offset %d claims size %d, below the %d-byte minimum",
				pos+consumed, totalSize, recordBatchOverhead)
		}
		if consumed+totalSize > length {
			return 0, fallback, fmt.Errorf(
				"capToHW: batch at offset %d claims size %d, exceeding the %d bytes available",
				pos+consumed, totalSize, length-consumed)
		}

		batchEnd := baseOffset + uint64(lastOffsetDelta) + 1
		if batchEnd > maxOffset {
			break
		}
		consumed += totalSize
		exclusiveEnd = batchEnd
	}

	return consumed, exclusiveEnd, nil
}
