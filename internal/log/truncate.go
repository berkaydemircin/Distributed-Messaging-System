package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (l *Log) TruncateTo(offset uint64) error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	active := l.activePair.Load()
	leo := active.segment.NextOffset()
	if offset > leo {
		return fmt.Errorf("TruncateTo: offset %d exceeds log end offset %d", offset, leo)
	}

	sealed := l.sealedPairs.Load().([]*segmentPair)
	oldest := active.segment.BaseOffset()
	if len(sealed) > 0 {
		oldest = sealed[0].segment.BaseOffset()
	}
	if offset < oldest {
		return fmt.Errorf("TruncateTo: offset %d is before log start %d", offset, oldest)
	}

	needsPhysicalTruncation := offset < leo
	var cleanupErr error

	if needsPhysicalTruncation {
		target := findSegment(sealed, active, offset)
		if target == nil {
			return fmt.Errorf("TruncateTo: no segment found for offset %d", offset)
		}

		position, err := findTruncationPosition(target.segment, offset)
		if err != nil {
			return fmt.Errorf("TruncateTo: %w", err)
		}

		if target == active {
			if err := truncateSegmentPair(target, position, offset, l.config.IndexIntervalBytes); err != nil {
				return fmt.Errorf("TruncateTo: %w", err)
			}
		} else {
			targetIndex := -1
			for i, pair := range sealed {
				if pair == target {
					targetIndex = i
					break
				}
			}
			if targetIndex < 0 {
				return fmt.Errorf("TruncateTo: target segment for offset %d is not in the sealed list", offset)
			}

			if err := truncateSegmentPair(target, position, offset, l.config.IndexIntervalBytes); err != nil {
				return fmt.Errorf("TruncateTo: %w", err)
			}

			toRemove := make([]*segmentPair, 0, len(sealed)-targetIndex)
			toRemove = append(toRemove, sealed[targetIndex+1:]...)
			toRemove = append(toRemove, active)

			newSealed := make([]*segmentPair, targetIndex)
			copy(newSealed, sealed[:targetIndex])

			l.sealedPairs.Store(newSealed)
			l.activePair.Store(target)

			for _, pair := range toRemove {
				if err := closeAndRemovePair(l.dir, pair); err != nil {
					cleanupErr = errors.Join(cleanupErr, err)
				}
			}
		}
	}

	l.epochMu.Lock()
	if needsPhysicalTruncation {
		l.epochCache.TruncateFromEnd(offset)
	}
	persistErr := l.epochCheckpoint.Write(l.epochCache.Entries())
	l.epochMu.Unlock()

	if persistErr != nil {
		persistErr = fmt.Errorf("persist epoch checkpoint: %w", persistErr)
	}
	if err := errors.Join(cleanupErr, persistErr); err != nil {
		return fmt.Errorf("TruncateTo: %w", err)
	}

	return nil
}

func truncateSegmentPair(
	pair *segmentPair,
	position int64,
	nextOffset uint64,
	indexIntervalBytes int64,
) error {
	if err := pair.segment.truncateAt(position, nextOffset); err != nil {
		return err
	}

	bytesSinceIndex, err := rebuildIndexFromSegment(
		pair.segment,
		pair.index,
		indexIntervalBytes,
	)
	if err != nil {
		return fmt.Errorf("rebuild index: %w", err)
	}
	pair.bytesSinceIndex = bytesSinceIndex
	return nil
}

func findTruncationPosition(seg *Segment, offset uint64) (int64, error) {
	size := seg.Size()
	var position int64

	for position < size {
		firstOffset, lastOffsetDelta, onDiskSize, err := seg.ReadBatchMetaAt(position)
		if err != nil {
			return 0, fmt.Errorf(
				"read batch at position %d in segment %d: %w",
				position,
				seg.BaseOffset(),
				err,
			)
		}
		if onDiskSize <= 0 || position+onDiskSize > size {
			return 0, fmt.Errorf(
				"invalid batch size %d at position %d in segment %d",
				onDiskSize,
				position,
				seg.BaseOffset(),
			)
		}

		if firstOffset == offset {
			return position, nil
		}

		lastOffset := firstOffset + uint64(lastOffsetDelta)
		if offset > firstOffset && offset <= lastOffset {
			return 0, fmt.Errorf(
				"offset %d falls inside batch [%d, %d] in segment %d",
				offset,
				firstOffset,
				lastOffset,
				seg.BaseOffset(),
			)
		}

		position += onDiskSize
	}

	if offset == seg.NextOffset() {
		return position, nil
	}

	return 0, fmt.Errorf(
		"offset %d is not a batch boundary in segment %d",
		offset,
		seg.BaseOffset(),
	)
}

func closeAndRemovePair(dir string, pair *segmentPair) error {
	baseOffset := pair.segment.BaseOffset()
	var errs []error

	if err := pair.index.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close index %d: %w", baseOffset, err))
	}
	if err := pair.segment.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close segment %d: %w", baseOffset, err))
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	if err := os.Remove(logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("remove segment file %d: %w", baseOffset, err))
	}

	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))
	if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("remove index file %d: %w", baseOffset, err))
	}

	return errors.Join(errs...)
}