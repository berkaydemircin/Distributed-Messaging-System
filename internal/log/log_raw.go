package log

import (
	"errors"
	"fmt"
)

func (l *Log) AppendRaw(data []byte, leaderEpoch int32) (baseOffset uint64, lastOffsetDelta uint32, err error) {
	bounds, err := scanRawBatches(data)
	if err != nil {
		return 0, 0, err
	}
	if len(bounds) == 0 {
		return 0, 0, fmt.Errorf("%w: empty records", ErrCorruptBatch)
	}

	for i, b := range bounds {
		if b.size > l.config.MaxMessageBytes {
			return 0, 0, fmt.Errorf("%w: batch %d/%d is %d bytes, exceeds MaxMessageBytes=%d",
				ErrMessageTooLarge, i+1, len(bounds), b.size, l.config.MaxMessageBytes)
		}
	}

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	active := l.activePair.Load()

	if active.segment.IsFull() || active.index.IsFull() {
		active, err = l.roll()
		if err != nil {
			return 0, 0, fmt.Errorf("AppendRaw: roll: %w", err)
		}
	}

	assignedOffset, pos, totalRecords, aErr := active.segment.AppendRawBatches(data, bounds, leaderEpoch)
	if errors.Is(aErr, ErrSegmentFull) {
		active, err = l.roll()
		if err != nil {
			return 0, 0, fmt.Errorf("AppendRaw: roll after full: %w", err)
		}
		assignedOffset, pos, totalRecords, aErr = active.segment.AppendRawBatches(data, bounds, leaderEpoch)
	}
	if aErr != nil {
		return 0, 0, fmt.Errorf("AppendRaw: %w", aErr)
	}

	active.bytesSinceIndex += int64(len(data))
	if pos == 0 || active.bytesSinceIndex >= l.config.IndexIntervalBytes {
		if err := active.index.Write(assignedOffset, pos); err != nil {
			return 0, 0, fmt.Errorf("AppendRaw: index write: %w", err)
		}
		active.bytesSinceIndex = 0
	}

	return assignedOffset, uint32(totalRecords - 1), nil
}

func (l *Log) ReadRaw(offset uint64, maxBytes int32) ([]byte, uint64, error) {
	active := l.activePair.Load()
	sealed := l.sealedPairs.Load().([]*segmentPair)

	pair := findSegment(sealed, active, offset)
	if pair == nil {
		return nil, 0, fmt.Errorf("ReadRaw: offset %d is before log start", offset)
	}

	filePos, err := pair.index.Lookup(offset)
	if err != nil {
		return nil, 0, fmt.Errorf("ReadRaw: index lookup for offset %d: %w", offset, err)
	}

	for {
		firstOffset, lastOffsetDelta, onDiskSize, err := pair.segment.ReadBatchMetaAt(filePos)
		if err != nil {
			return nil, 0, fmt.Errorf("ReadRaw: meta at pos %d: %w", filePos, err)
		}

		batchEnd := firstOffset + uint64(lastOffsetDelta)

		if offset >= firstOffset && offset <= batchEnd {
			return pair.segment.ReadRawBatchAt(filePos, maxBytes)
		}
		if offset < firstOffset {
			return nil, 0, fmt.Errorf("ReadRaw: offset %d not found (gap after %d)", offset, batchEnd)
		}

		nextPos := filePos + onDiskSize
		if nextPos >= pair.segment.Size() {
			return nil, 0, fmt.Errorf("ReadRaw: offset %d not found: end of segment", offset)
		}
		filePos = nextPos
	}
}

func (l *Log) ReadRawRanges(offset uint64, maxBytes int32) (ReadRawRangesResult, error) {
	active := l.activePair.Load()
	sealed := l.sealedPairs.Load().([]*segmentPair)

	pair := findSegment(sealed, active, offset)
	if pair == nil {
		return ReadRawRangesResult{}, fmt.Errorf("ReadRawRanges: offset %d is before log start", offset)
	}

	filePos, err := pair.index.Lookup(offset)
	if err != nil {
		return ReadRawRangesResult{}, fmt.Errorf("ReadRawRanges: index lookup for offset %d: %w", offset, err)
	}

	for {
		firstOffset, lastOffsetDelta, onDiskSize, err := pair.segment.ReadBatchMetaAt(filePos)
		if err != nil {
			return ReadRawRangesResult{}, fmt.Errorf("ReadRawRanges: meta at pos %d: %w", filePos, err)
		}

		batchEnd := firstOffset + uint64(lastOffsetDelta)

		if offset >= firstOffset && offset <= batchEnd {
			return pair.segment.ReadRawRangesAt(filePos, maxBytes)
		}
		if offset < firstOffset {
			return ReadRawRangesResult{}, fmt.Errorf("ReadRawRanges: offset %d not found (gap before batch starting at %d)", offset, firstOffset)
		}

		nextPos := filePos + onDiskSize
		if nextPos >= pair.segment.Size() {
			return ReadRawRangesResult{}, fmt.Errorf("ReadRawRanges: offset %d not found: end of segment", offset)
		}
		filePos = nextPos
	}
}
