package log

import "fmt"

func (l *Log) AssignLeaderEpoch(epoch int32, startOffset uint64) error {
	l.epochMu.Lock()
	defer l.epochMu.Unlock()

	if _, err := l.epochCache.Assign(epoch, startOffset); err != nil {
		return fmt.Errorf("assign leader epoch: %w", err)
	}
	if err := l.epochCheckpoint.Write(l.epochCache.Entries()); err != nil {
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
