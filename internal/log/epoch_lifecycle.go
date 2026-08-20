package log

import "fmt"

func (l *Log) persistEpochCheckpointLocked() error {
	if !l.epochCheckpointDirty {
		return nil
	}
	if err := l.epochCheckpoint.Write(l.epochCache.Entries()); err != nil {
		return err // leave dirty=true so a retry writes again
	}
	l.epochCheckpointDirty = false
	return nil
}

func (l *Log) AssignLeaderEpoch(epoch int32, startOffset uint64) error {
	l.epochMu.Lock()
	defer l.epochMu.Unlock()

	changed, err := l.epochCache.Assign(epoch, startOffset)
	if err != nil {
		return fmt.Errorf("assign leader epoch: %w", err)
	}
	if changed {
		l.epochCheckpointDirty = true
	}

	if err := l.persistEpochCheckpointLocked(); err != nil {
		return fmt.Errorf("persist leader epoch checkpoint: %w", err)
	}

	return nil
}

func (l *Log) assignLeaderEpochs(entries []EpochEntry) error {
	l.epochMu.Lock()
	defer l.epochMu.Unlock()

	for _, e := range entries {
		changed, err := l.epochCache.Assign(e.Epoch, e.StartOffset)
		if err != nil {
			return fmt.Errorf("assign leader epoch %d at offset %d: %w", e.Epoch, e.StartOffset, err)
		}
		if changed {
			l.epochCheckpointDirty = true
		}
	}

	if err := l.persistEpochCheckpointLocked(); err != nil {
		return fmt.Errorf("persist leader epoch checkpoint: %w", err)
	}

	return nil
}

func (l *Log) EndOffsetForLeaderEpoch(epoch int32) (EpochEndOffset, bool) {
	return l.epochCache.EndOffsetFor(epoch, l.NextOffset())
}

func (l *Log) EpochForOffset(offset uint64) (int32, bool) {
	return l.epochCache.EpochForOffset(offset)
}

func (l *Log) LatestLeaderEpoch() (int32, bool) {
	entries := l.epochCache.Entries()
	if len(entries) == 0 {
		return 0, false
	}

	return entries[len(entries)-1].Epoch, true
}
