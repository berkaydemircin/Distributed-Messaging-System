package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

// defaults for config
const (
	defaultMaxSegmentBytes    = 1 << 30 // 1 gb
	defaultMaxIndexBytes      = 1 << 20 // 1 mb
	defaultIndexIntervalBytes = 4096    // 4 kb
)

type LogConfig struct {
	MaxSegmentBytes    int64 // max bytes per .log segment file before rolling (default 1gb)
	MaxIndexBytes      int64 // max bytes per .index file (default 1mb)
	IndexIntervalBytes int64 // min log bytes between index entries. lower = denser index (default 4096)
}

type segmentPair struct {
	segment         *Segment
	index           *Index
	bytesSinceIndex int64 // bytes appended to .log since last index write (for sparse indexing)
}

/*
 * Log is the partition level log manager
 * Concurrency model:
 * 	- Writes are serialized by writeMu (one writer per partition).
 * 	- Reads are lock free. activePair and sealedPairs are accessed atomically.
 *	- Sealed segments are immutable.
 */
type Log struct {
	dir    string
	config LogConfig

	writeMu sync.Mutex

	activePair  atomic.Pointer[segmentPair]
	sealedPairs atomic.Value
}

func applyDefaults(config *LogConfig) {
	if config.MaxSegmentBytes <= 0 {
		config.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if config.MaxIndexBytes <= 0 {
		config.MaxIndexBytes = defaultMaxIndexBytes
	}
	if config.IndexIntervalBytes <= 0 {
		config.IndexIntervalBytes = defaultIndexIntervalBytes
	}
}

func NewLog(dir string, config LogConfig) (*Log, error) {
	applyDefaults(&config)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	baseOffsets, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}

	l := &Log{
		dir:    dir,
		config: config,
	}
	l.sealedPairs.Store([]*segmentPair{})

	if len(baseOffsets) == 0 {
		pair, err := newSegmentPair(dir, 0, config)
		if err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
		l.activePair.Store(pair)
		return l, nil
	}

	sealed := make([]*segmentPair, 0, len(baseOffsets)-1)
	for i, offset := range baseOffsets {
		pair, err := newSegmentPair(dir, offset, config)
		if err != nil {
			closeAll(sealed)
			return nil, fmt.Errorf("open segment at offset %d: %w", offset, err)
		}
		if i < len(baseOffsets)-1 {
			sealed = append(sealed, pair)
		} else {
			l.activePair.Store(pair)
		}
	}
	l.sealedPairs.Store(sealed)

	return l, nil
}

func (l *Log) Append(batch *protocol.Batch) (uint64, error) {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	active := l.activePair.Load()

	if active.segment.IsFull() || active.index.IsFull() {
		var err error
		active, err = l.roll()
		if err != nil {
			return 0, fmt.Errorf("roll segment: %w", err)
		}
	}

	pos, err := active.segment.AppendBatch(batch)
	if errors.Is(err, ErrSegmentFull) {
		if active.segment.Size() == 0 {
			return 0, fmt.Errorf("batch too large to fit in a (any) segment: max allowed=%d", l.config.MaxSegmentBytes)
		}
		active, err = l.roll()
		if err != nil {
			return 0, fmt.Errorf("roll segment: %w", err)
		}
		pos, err = active.segment.AppendBatch(batch)
	}
	if err != nil {
		return 0, err
	}

	batchBytes := int64(encodedBatchSize(batch))
	active.bytesSinceIndex += batchBytes

	if pos == 0 || active.bytesSinceIndex >= l.config.IndexIntervalBytes {
		_ = active.index.Write(batch.FirstOffset, pos)
		active.bytesSinceIndex = 0
	}

	return batch.FirstOffset, nil
}

func (l *Log) Read(offset uint64) (*protocol.Batch, error) {
	active := l.activePair.Load()
	sealed := l.sealedPairs.Load().([]*segmentPair)

	pair := findSegment(sealed, active, offset)
	if pair == nil {
		return nil, fmt.Errorf("offset %d is before the start of the log", offset)
	}

	filePos, err := pair.index.Lookup(offset)
	if err != nil {
		return nil, fmt.Errorf("index lookup for offset %d: %w", offset, err)
	}

	// linear scan
	for {
		firstOffset, lastOffsetDelta, onDiskSize, err := pair.segment.ReadBatchMetaAt(filePos)
		if err != nil {
			return nil, fmt.Errorf("read batch meta at pos %d: %w", filePos, err)
		}

		batchEnd := firstOffset + uint64(lastOffsetDelta)

		if offset >= firstOffset && offset <= batchEnd {
			return pair.segment.ReadBatchAt(filePos)
		}

		if offset < firstOffset {
			return nil, fmt.Errorf("offset %d not found in segment (gap after offset %d)",
				offset, batchEnd)
		}

		nextPos := filePos + onDiskSize
		if nextPos >= pair.segment.Size() {
			return nil, fmt.Errorf("offset %d not found: reached end of segment", offset)
		}
		filePos = nextPos
	}
}

/*
 * flushes and closes every segment and index in the log.
 */
func (l *Log) Close() error {
	l.writeMu.Lock()
	defer l.writeMu.Unlock()

	var firstErr error
	collect := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if active := l.activePair.Load(); active != nil {
		collect(active.index.Close())
		collect(active.segment.Close())
	}

	for _, pair := range l.sealedPairs.Load().([]*segmentPair) {
		collect(pair.index.Close())
		collect(pair.segment.Close())
	}

	return firstErr
}

func (l *Log) OldestOffset() uint64 {
	sealed := l.sealedPairs.Load().([]*segmentPair)
	if len(sealed) > 0 {
		return sealed[0].segment.BaseOffset()
	}
	return l.activePair.Load().segment.BaseOffset()
}

func (l *Log) NextOffset() uint64 {
	return l.activePair.Load().segment.NextOffset()
}

/*
 * seals the current active segment and creates a new one
 * IMPORTANT: Must be called under writeMu
 */
func (l *Log) roll() (*segmentPair, error) {
	old := l.activePair.Load()
	nextOffset := old.segment.NextOffset()

	newPair, err := newSegmentPair(l.dir, nextOffset, l.config)
	if err != nil {
		return nil, err
	}

	sealed := l.sealedPairs.Load().([]*segmentPair)
	updated := make([]*segmentPair, len(sealed)+1)
	copy(updated, sealed)
	updated[len(sealed)] = old
	l.sealedPairs.Store(updated)

	l.activePair.Store(newPair)
	return newPair, nil
}

func newSegmentPair(dir string, baseOffset uint64, config LogConfig) (*segmentPair, error) {
	seg, err := NewSegment(dir, baseOffset, config.MaxSegmentBytes)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	idx, err := NewIndex(dir, baseOffset, config.MaxIndexBytes)
	if err != nil {
		seg.Close()
		return nil, fmt.Errorf("open index: %w", err)
	}
	return &segmentPair{segment: seg, index: idx}, nil
}

/*
 * returns the segmentPair that owns the given offset.
 * Uses binary search on the sealed slice, then checks the active pair.
 */
func findSegment(sealed []*segmentPair, active *segmentPair, offset uint64) *segmentPair {
	if offset >= active.segment.BaseOffset() {
		return active
	}

	// Binary search, find the last sealed segment with baseOffset <= offset
	i := sort.Search(len(sealed), func(i int) bool {
		return sealed[i].segment.BaseOffset() > offset
	}) - 1

	if i < 0 {
		return nil
	}
	return sealed[i]
}

/*
 * globs for .log files in dir and returns their base offsets in sorted ascending order
 */
func discoverSegments(dir string) ([]uint64, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("glob log files: %w", err)
	}

	offsets := make([]uint64, 0, len(matches))
	for _, match := range matches {
		name := strings.TrimSuffix(filepath.Base(match), ".log")
		var offset uint64
		if _, err := fmt.Sscanf(name, "%d", &offset); err != nil {
			continue
		}
		offsets = append(offsets, offset)
	}

	slices.Sort(offsets)
	return offsets, nil
}

func closeAll(pairs []*segmentPair) {
	for _, p := range pairs {
		p.segment.Close()
		p.index.Close()
	}
}
