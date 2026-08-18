package log

import (
	"fmt"
	"sync"
)

type EpochEntry struct {
	Epoch       int32
	StartOffset uint64
}

type EpochEndOffset struct {
	Epoch     int32
	EndOffset uint64
}

type LeaderEpochCache struct {
	mu      sync.RWMutex
	entries []EpochEntry
}

func NewLeaderEpochCache() *LeaderEpochCache {
	return &LeaderEpochCache{}
}

func (c *LeaderEpochCache) Assign(epoch int32, startOffset uint64) (changed bool, err error) {
	if epoch < 0 {
		return false, fmt.Errorf("leader epoch: negative epoch %d", epoch)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	n := len(c.entries)
	if n == 0 {
		c.entries = append(c.entries, EpochEntry{Epoch: epoch, StartOffset: startOffset})
		return true, nil
	}

	last := c.entries[n-1]
	if epoch == last.Epoch && startOffset >= last.StartOffset {
		return false, nil
	}

	if epoch > last.Epoch && startOffset > last.StartOffset {
		c.entries = append(c.entries, EpochEntry{Epoch: epoch, StartOffset: startOffset})
		return true, nil
	}

	cut := 0
	for cut < len(c.entries) {
		e := c.entries[cut]
		if e.Epoch >= epoch || e.StartOffset >= startOffset {
			break
		}
		cut++
	}
	c.entries = append(c.entries[:cut], EpochEntry{Epoch: epoch, StartOffset: startOffset})
	return true, nil
}

func (c *LeaderEpochCache) TruncateFromEnd(offset uint64) (changed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := 0
	for i < len(c.entries) && c.entries[i].StartOffset < offset {
		i++
	}
	if i == len(c.entries) {
		return false
	}
	c.entries = c.entries[:i]
	return true
}

func (c *LeaderEpochCache) EpochForOffset(offset uint64) (int32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var epoch int32
	found := false

	for _, entry := range c.entries {
		if entry.StartOffset > offset {
			break
		}

		epoch = entry.Epoch
		found = true
	}

	return epoch, found
}

func (c *LeaderEpochCache) EndOffsetFor(requestedEpoch int32, leo uint64) (EpochEndOffset, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.entries) == 0 || requestedEpoch < 0 {
		return EpochEndOffset{}, false
	}

	first := c.entries[0]
	if requestedEpoch < first.Epoch {
		return EpochEndOffset{
			Epoch:     requestedEpoch,
			EndOffset: first.StartOffset,
		}, true
	}

	last := c.entries[len(c.entries)-1]
	if requestedEpoch == last.Epoch {
		return EpochEndOffset{
			Epoch:     last.Epoch,
			EndOffset: leo,
		}, true
	}
	if requestedEpoch > last.Epoch {
		return EpochEndOffset{}, false
	}

	for i := len(c.entries) - 1; i >= 0; i-- {
		if c.entries[i].Epoch <= requestedEpoch {
			return EpochEndOffset{
				Epoch:     c.entries[i].Epoch,
				EndOffset: c.entries[i+1].StartOffset,
			}, true
		}
	}

	return EpochEndOffset{}, false
}

func (c *LeaderEpochCache) TruncateAfter(offset uint64) (changed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	i := 0
	for i < len(c.entries) && c.entries[i].StartOffset <= offset {
		i++
	}
	if i == len(c.entries) {
		return false
	}

	c.entries = c.entries[:i]
	return true
}

func (c *LeaderEpochCache) Entries() []EpochEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]EpochEntry, len(c.entries))
	copy(out, c.entries)
	return out
}
