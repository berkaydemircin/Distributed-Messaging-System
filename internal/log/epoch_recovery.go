package log

import "fmt"

func (l *Log) ScanEpochHistory() ([]EpochEntry, error) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	return l.scanEpochHistoryLocked()
}

// require l.WriteMutex to be held
func (l *Log) scanEpochHistoryLocked() ([]EpochEntry, error) {
	sealed := l.sealedPairs.Load().([]*segmentPair)
	pairs := make([]*segmentPair, 0, len(sealed)+1)
	pairs = append(pairs, sealed...)
	if active := l.activePair.Load(); active != nil {
		pairs = append(pairs, active)
	}

	var entries []EpochEntry
	var lastEpoch int32
	haveLastEpoch := false

	for _, pair := range pairs {
		size := pair.segment.Size()
		for pos := int64(0); pos < size; {
			firstOffset, epoch, onDiskSize, err := pair.segment.readBatchOffsetAndEpochAt(pos)
			if err != nil {
				return nil, fmt.Errorf(
					"ScanEpochHistory: segment base offset %d: %w",
					pair.segment.BaseOffset(),
					err,
				)
			}

			if onDiskSize > size-pos {
				return nil, fmt.Errorf(
					"ScanEpochHistory: batch at position %d in segment %d has size %d, exceeding %d bytes remaining",
					pos,
					pair.segment.BaseOffset(),
					onDiskSize,
					size-pos,
				)
			}

			if epoch >= 0 && (!haveLastEpoch || epoch != lastEpoch) {
				entries = append(entries, EpochEntry{
					Epoch:       epoch,
					StartOffset: firstOffset,
				})
				lastEpoch = epoch
				haveLastEpoch = true
			}

			pos += onDiskSize
		}
	}

	return entries, nil
}

func (l *Log) RecoverEpochCache(checkpoint *EpochCheckpoint) (*LeaderEpochCache, error) {
	if checkpoint == nil {
		return nil, fmt.Errorf("RecoverEpochCache: nil checkpoint")
	}

	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	leo := l.NextOffset()
	loaded, err := checkpoint.Read()
	if err != nil {
		return nil, fmt.Errorf("RecoverEpochCache: reading checkpoint: %w", err)
	}

	cache, err := replayEpochEntries(loaded)
	if err != nil {
		return nil, fmt.Errorf("RecoverEpochCache: replaying checkpoint: %w", err)
	}

	repaired := !epochEntriesEqual(loaded, cache.Entries())
	cache.TruncateAfter(leo)

	usedScan := false
	if leo > 0 && (repaired || len(cache.Entries()) == 0) {
		scanned, err := l.scanEpochHistoryLocked()
		if err != nil {
			return nil, fmt.Errorf("RecoverEpochCache: scanning log: %w", err)
		}

		cache, err = replayEpochEntries(scanned)
		if err != nil {
			return nil, fmt.Errorf("RecoverEpochCache: replaying scanned history: %w", err)
		}
		cache.TruncateAfter(leo)
		usedScan = true
	}

	normalized := cache.Entries()
	if usedScan || !epochEntriesEqual(loaded, normalized) {
		if err := checkpoint.Write(normalized); err != nil {
			return nil, fmt.Errorf("RecoverEpochCache: rewriting checkpoint: %w", err)
		}
	}

	return cache, nil
}

func replayEpochEntries(entries []EpochEntry) (*LeaderEpochCache, error) {
	cache := NewLeaderEpochCache()
	for _, entry := range entries {
		if _, err := cache.Assign(entry.Epoch, entry.StartOffset); err != nil {
			return nil, err
		}
	}
	return cache, nil
}

func epochEntriesEqual(a, b []EpochEntry) bool {
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
