package log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	/*	BatchHeaderSize is the fixed byte size of every batch header on disk.

			Layout:
			firstOffset     uint64   8 bytes  — absolute offset of first message
		    batchLength     uint32   4 bytes  — byte length from byte 16 to end
			crc             uint32   4 bytes  — crc32 of bytes 16 onward
			attributes      uint16   2 bytes  — compression codec, flags
			lastOffsetDelta uint32   4 bytes  — (lastOffset - firstOffset)
			firstTimestamp  int64    8 bytes  — unix ms of earliest message
			maxTimestamp    int64    8 bytes  — unix ms of latest message
	*/
	BatchHeaderSize = 38

	/* MessageHeaderSize is the fixed per message overhead within a batch.

	Layout:
	length        uint32   4 bytes  — byte length of everything after this
	attributes    uint8    1 byte   — reserved, currently always 0. I read that Kafka has this, will explore later.
	offsetDelta   uint32   4 bytes  — (thisOffset - batch.firstOffset)
	timestamp     int64    8 bytes  — unix ms for this message
	keyLength     uint32   4 bytes  — key byte length (max 4GB but this would be absurd)
	valueLength   uint32   4 bytes  — value byte length (max 4GB)
	*/
	MessageHeaderSize = 25
)

type Segment struct {
	file        *os.File
	baseOffset  uint64
	nextOffset  uint64
	currentSize int64
	maxSize     int64 // kept as int instead of uint for compatibility with other methods
}

/*
 * Opens (if exists) or creates and returns a new segment at offset.
 * o.w. returns error and terminates. Does not return the existing file.
 */
func NewSegment(dir string, baseOffset uint64, maxSize int64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	segment := &Segment{
		file:        file,
		baseOffset:  baseOffset,
		nextOffset:  baseOffset,
		currentSize: 0,
		maxSize:     maxSize,
	}

	// handles if file already exists
	if err := segment.recover(); err != nil {
		file.Close()
		return nil, fmt.Errorf("recover segment: %w", err)
	}

	// file did not exist
	return segment, nil
}

/* Recover from disk
 *
 */
func (segment *Segment) recover() error {
	info, err := segment.file.Stat()
	if err != nil {
		return err
	}

	segment.currentSize = info.Size()
	if segment.currentSize == 0 {
		return nil
	}

	var pos int64 = 0
	header := make([]byte, BatchHeaderSize)

	for pos < segment.currentSize {
		n, err := segment.file.ReadAt(header, pos)
		if err != nil {
			if err == io.EOF || n < BatchHeaderSize {
				if err := segment.file.Truncate(pos); err != nil {
					return fmt.Errorf("truncate partial header at %d: %w", pos, err)
				}
				segment.currentSize = pos
				break
			}
			return fmt.Errorf("read header at %d: %w", pos, err)
		}

		batchLength := binary.BigEndian.Uint32(header[8:12])
		lastOffsetDelta := binary.BigEndian.Uint32(header[18:22])
		batchEnd := pos + 16 + int64(batchLength)

		if batchEnd > segment.currentSize {
			if err := segment.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate partial batch at %d: %w", pos, err)
			}
			segment.currentSize = pos
			break
		}
		segment.nextOffset += uint64(lastOffsetDelta) + 1
		pos = batchEnd
	}

	if (segment.nextOffset == segment.baseOffset) && segment.currentSize > 0 {
		return fmt.Errorf("corrupt segment: non empty file but no valid batches %w", err)
	}

	return nil
}
