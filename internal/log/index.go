package log

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
)

const (
	// 4 (relOffset) + 8 (filePos) = 12 bytes
	indexEntrySize = 12

	/*
	 * preallocates 1 mb per index file
	 * at 12 bytes per entry this holds ~87.381 entries.
	 */
	defaultIndexMaxBytes = 1024 * 1024
)

type Index struct {
	file       *os.File
	mmap       []byte
	baseOffset uint64
	size       atomic.Int64 // bytes of mmap actually written; atomic for lock-free reads
	maxBytes   int64
}

// Opens or creates the index file for the segment at baseOffset
func NewIndex(dir string, baseOffset uint64, maxBytes int64) (*Index, error) {
	if maxBytes == 0 {
		maxBytes = defaultIndexMaxBytes
	}

	path := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open index %s: %w", path, err)
	}

	if err := file.Truncate(maxBytes); err != nil {
		file.Close()
		return nil, fmt.Errorf("pre-allocate index %s: %w", path, err)
	}

	// map_private would make it so the changes in memory dont end up on disk
	mmap, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(maxBytes),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("mmap index %s: %w", path, err)
	}

	idx := &Index{
		file:       file,
		mmap:       mmap,
		baseOffset: baseOffset,
		maxBytes:   maxBytes,
	}

	idx.size.Store(idx.recoverSize())
	return idx, nil
}

// scans the mmap to find where written entries end
func (idx *Index) recoverSize() int64 {
	var pos int64 = 0
	for pos+indexEntrySize <= idx.maxBytes {
		relOffset := binary.BigEndian.Uint32(idx.mmap[pos : pos+4])
		filePos := binary.BigEndian.Uint64(idx.mmap[pos+4 : pos+12])

		if pos > 0 && relOffset == 0 && filePos == 0 {
			break
		}
		pos += indexEntrySize
	}
	return pos
}

func (idx *Index) Write(offset uint64, filePos int64) error {
	if idx.IsFull() {
		return fmt.Errorf("index full: size=%d maxBytes=%d", idx.size.Load(), idx.maxBytes)
	}

	cur := idx.size.Load()
	relOffset := uint32(offset - idx.baseOffset)
	binary.BigEndian.PutUint32(idx.mmap[cur:cur+4], relOffset)
	binary.BigEndian.PutUint64(idx.mmap[cur+4:cur+12], uint64(filePos))

	idx.size.Add(indexEntrySize)
	return nil
}

/*
 * Returns the file position of the batch whose firstOffset is the largest indexed offset <= target,
 * 0 if the index is empty
 */
func (idx *Index) Lookup(offset uint64) (int64, error) {
	curSize := idx.size.Load()
	if curSize == 0 {
		return 0, nil
	}
	if offset < idx.baseOffset {
		return 0, fmt.Errorf("offset %d is below segment baseOffset %d",
			offset, idx.baseOffset)
	}

	relTarget := uint32(offset - idx.baseOffset)
	numEntries := int(curSize / indexEntrySize)

	low, high := 0, numEntries-1
	result := 0

	for low <= high {
		mid := (low + high) / 2
		entryPos := int64(mid) * indexEntrySize
		relOffset := binary.BigEndian.Uint32(idx.mmap[entryPos : entryPos+4])

		if relOffset <= relTarget {
			// valid candidate but search for a better one (if it exists)
			result = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	entryPos := int64(result) * indexEntrySize
	filePos := int64(binary.BigEndian.Uint64(idx.mmap[entryPos+4 : entryPos+12]))
	return filePos, nil
}

func (idx *Index) Close() error {
	if err := syscall.Munmap(idx.mmap); err != nil {
		return fmt.Errorf("munmap: %w", err)
	}
	idx.mmap = nil

	if err := idx.file.Truncate(idx.size.Load()); err != nil {
		return fmt.Errorf("truncate on close: %w", err)
	}

	return idx.file.Close()
}

func (idx *Index) IsFull() bool       { return idx.size.Load()+indexEntrySize > idx.maxBytes }
func (idx *Index) Size() int64        { return idx.size.Load() }
func (idx *Index) BaseOffset() uint64 { return idx.baseOffset }
