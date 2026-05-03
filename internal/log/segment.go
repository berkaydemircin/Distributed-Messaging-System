package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/berkaydemircin/Distributed-Messaging-System/internal/protocol"
)

var ErrSegmentFull = errors.New("segment full")

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

	/*	MessageHeaderSize is the fixed per message overhead within a batch.

		Layout:
		length        uint32   4 bytes  — byte length of everything after this
		attributes    uint8    1 byte   — reserved, currently always 0.
		offsetDelta   uint32   4 bytes  — (thisOffset - batch.firstOffset)
		timestamp     int64    8 bytes  — unix ms for this message
		keyLength     uint32   4 bytes  — key byte length
		valueLength   uint32   4 bytes  — value byte length
	*/
	MessageHeaderSize = 25
)

type Segment struct {
	file        *os.File
	baseOffset  uint64
	nextOffset  atomic.Uint64
	currentSize atomic.Int64
	maxSize     int64
}

/*
 * Opens (if exists) or creates a new segment at baseOffset and rebuilds its
 * state in memory by scanning the file.
 */
func NewSegment(dir string, baseOffset uint64, maxSize int64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	segment := &Segment{
		file:    file,
		baseOffset: baseOffset,
		maxSize: maxSize,
	}
	segment.nextOffset.Store(baseOffset)
	segment.currentSize.Store(0)

	if err := segment.recover(); err != nil {
		file.Close()
		return nil, fmt.Errorf("recover segment: %w", err)
	}

	// file did not exist
	return segment, nil
}

func (segment *Segment) recover() error {
	info, err := segment.file.Stat()
	if err != nil {
		return err
	}

	fileSize := info.Size()
	if fileSize == 0 {
		return nil
	}

	var pos int64
	header := make([]byte, BatchHeaderSize)

	for pos < fileSize {
		n, err := segment.file.ReadAt(header, pos)
		if err != nil {
			if err == io.EOF || n < BatchHeaderSize {
				// crashed mid writing the header
				if err := segment.file.Truncate(pos); err != nil {
					return fmt.Errorf("truncate partial header at %d: %w", pos, err)
				}
				break
			}
			return fmt.Errorf("read header at %d: %w", pos, err)
		}

		batchLength := binary.BigEndian.Uint32(header[8:12])
		lastOffsetDelta := binary.BigEndian.Uint32(header[18:22])
		batchEnd := pos + 16 + int64(batchLength)

		if batchEnd > fileSize {
			if err := segment.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate partial batch at %d: %w", pos, err)
			}
			break
		}

		body := make([]byte, batchLength)
		if _, err := segment.file.ReadAt(body, pos+16); err != nil {
			if err := segment.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate unreadable body at %d: %w", pos, err)
			}
			break
		}

		storedCRC := binary.BigEndian.Uint32(header[12:16])
		if crc32.ChecksumIEEE(body) != storedCRC {
			if err := segment.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate corrupt batch at %d: %w", pos, err)
			}
			break
		}

		// Batch is valid. Advance state and move to the next batch.
		segment.nextOffset.Add(uint64(lastOffsetDelta) + 1)
		pos = batchEnd
	}

	// pos is now the byte offset of the first invalid/missing batch,
	// which is the correct append position and the logical file size.
	segment.currentSize.Store(pos)

	if segment.nextOffset.Load() == segment.baseOffset && pos > 0 {
		return fmt.Errorf("corrupt segment: non-empty file but no valid batches")
	}

	return nil
}

func encodedBatchSize(batch *protocol.Batch) int {
	size := BatchHeaderSize
	for _, m := range batch.Messages {
		size += MessageHeaderSize
		size += len(m.Key)
		size += len(m.Value)
	}
	return size
}

/*
 * Serialized batch to on disk wire format
 */
func encodeBatch(batch *protocol.Batch) []byte {
	totalSize := encodedBatchSize(batch)
	buf := make([]byte, totalSize)

	// batch header
	content := buf[16:]
	binary.BigEndian.PutUint16(content[0:2], batch.Attributes)
	binary.BigEndian.PutUint32(content[2:6], batch.LastOffsetDelta)
	binary.BigEndian.PutUint64(content[6:14], uint64(batch.FirstTimestamp))
	binary.BigEndian.PutUint64(content[14:22], uint64(batch.MaxTimestamp))

	// messages
	cursor := 22
	for i, msg := range batch.Messages {
		msgStart := cursor
		cursor += 4 // length placeholder

		content[cursor] = 0 // attributes
		cursor++

		binary.BigEndian.PutUint32(content[cursor:], uint32(i)) // offsetDelta
		cursor += 4

		binary.BigEndian.PutUint64(content[cursor:], uint64(msg.Timestamp))
		cursor += 8

		binary.BigEndian.PutUint32(content[cursor:], uint32(len(msg.Key)))
		cursor += 4
		copy(content[cursor:], msg.Key)
		cursor += len(msg.Key)

		binary.BigEndian.PutUint32(content[cursor:], uint32(len(msg.Value)))
		cursor += 4
		copy(content[cursor:], msg.Value)
		cursor += len(msg.Value)

		binary.BigEndian.PutUint32(content[msgStart:], uint32(cursor-msgStart-4))
	}

	binary.BigEndian.PutUint64(buf[0:8], batch.FirstOffset)
	binary.BigEndian.PutUint32(buf[8:12], uint32(totalSize-16))
	binary.BigEndian.PutUint32(buf[12:16], crc32.ChecksumIEEE(buf[16:]))

	return buf
}

/*
 * Deserializes a batch body (bytes after the first 16) into a protocol.Batch.
 */
func decodeBatch(firstOffset uint64, body []byte) (*protocol.Batch, error) {

	bodyLength := len(body)

	if bodyLength < 22 {
		return nil, fmt.Errorf("batch body is too short: %d bytes", bodyLength)
	}

	batch := &protocol.Batch{FirstOffset: firstOffset}
	batch.Attributes = binary.BigEndian.Uint16(body[0:2])
	batch.LastOffsetDelta = binary.BigEndian.Uint32(body[2:6])
	batch.FirstTimestamp = int64(binary.BigEndian.Uint64(body[6:14]))
	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(body[14:22]))

	msgCount := int(batch.LastOffsetDelta) + 1
	batch.Messages = make([]protocol.Message, msgCount)

	cursor := 22
	for i := range msgCount {
		if cursor+4 > bodyLength {
			return nil, fmt.Errorf("truncated length field for msg %d", i)
		}

		msgBodyLen := int(binary.BigEndian.Uint32(body[cursor:]))
		cursor += 4

		if cursor+msgBodyLen > bodyLength {
			return nil, fmt.Errorf("message %d body truncated: need %d bytes, have %d", i, msgBodyLen, bodyLength-cursor)
		}

		message := &batch.Messages[i]
		msgBodyStart := cursor

		// skipping attr and offset
		if cursor+5 > bodyLength {
			return nil, fmt.Errorf("message %d header truncated", i)
		}
		cursor += 5

		if cursor+8 > bodyLength {
			return nil, fmt.Errorf("message %d timestamp truncated", i)
		}
		message.Timestamp = int64(binary.BigEndian.Uint64(body[cursor:]))
		cursor += 8

		if cursor+4 > bodyLength {
			return nil, fmt.Errorf("message %d key length field truncated", i)
		}
		keyLength := int(binary.BigEndian.Uint32(body[cursor:]))
		cursor += 4
		if keyLength > 0 {
			if cursor+keyLength > bodyLength {
				return nil, fmt.Errorf("message %d key truncated: need %d bytes, have %d",
					i, keyLength, bodyLength-cursor)
			}
			/* message.Key = make([]byte, keyLength)
			copy(message.Key, body[cursor:cursor+int(keyLength)])
			cursor += int(keyLength) */
			message.Key = body[cursor : cursor+keyLength : cursor+keyLength]
			cursor += keyLength

		}

		if cursor+4 > bodyLength {
			return nil, fmt.Errorf("message %d value length field truncated", i)
		}
		valueLength := int(binary.BigEndian.Uint32(body[cursor:]))
		cursor += 4
		if valueLength > 0 {
			if cursor+valueLength > bodyLength {
				return nil, fmt.Errorf("message %d value truncated: need %d bytes, have %d",
					i, valueLength, bodyLength-cursor)
			}
			/* message.Value = make([]byte, valueLength)
			copy(message.Value, body[cursor:cursor+int(valueLength)])
			cursor += int(valueLength) */
			message.Value = body[cursor : cursor+valueLength : cursor+valueLength]
			cursor += valueLength
		}

		// is an end of decode sanity check size comparison good here? im not sure if its necessary
		if cursor-msgBodyStart != msgBodyLen {
			return nil, fmt.Errorf("message %d length mismatch: declared %d bytes, consumed %d",
				i, msgBodyLen, cursor-msgBodyStart)
		}
	}

	return batch, nil
}

func (segment *Segment) AppendBatch(batch *protocol.Batch) (int64, error) {
	totalSize := encodedBatchSize(batch)
	if segment.currentSize.Load()+int64(totalSize) > segment.maxSize {
		return 0, ErrSegmentFull
	}

	if len(batch.Messages) == 0 {
		return 0, fmt.Errorf("empty batch")
	}

	batch.FirstOffset = segment.nextOffset.Load()
	batch.LastOffsetDelta = uint32(len(batch.Messages) - 1)

	buf := encodeBatch(batch)
	pos := segment.currentSize.Load()

	if _, err := segment.file.WriteAt(buf, pos); err != nil {
		return 0, fmt.Errorf("write batch at pos %d: %w", pos, err)
	}

	segment.nextOffset.Add(uint64(len(batch.Messages)))
	segment.currentSize.Add(int64(len(buf)))
	return pos, nil
}

func (segment *Segment) ReadBatchAt(pos int64) (*protocol.Batch, error) {
	var header [BatchHeaderSize]byte
	if _, err := segment.file.ReadAt(header[:], pos); err != nil {
		return nil, fmt.Errorf("read batch header at %d: %w", pos, err)
	}

	firstOffset := binary.BigEndian.Uint64(header[0:8])
	batchLength := binary.BigEndian.Uint32(header[8:12])
	storedCRC := binary.BigEndian.Uint32(header[12:16])

	body := make([]byte, batchLength)
	if _, err := segment.file.ReadAt(body, pos+16); err != nil {
		return nil, fmt.Errorf("read batch body at %d: %w", pos+16, err)
	}

	if crc32.ChecksumIEEE(body) != storedCRC {
		return nil, fmt.Errorf("crc mismatch at pos %d: batch corrupted", pos)
	}

	return decodeBatch(firstOffset, body)
}

func (segment *Segment) ReadBatchMetaAt(pos int64) (firstOffset uint64, lastOffsetDelta uint32, onDiskSize int64, err error) {
	var buf [22]byte
	if _, err = segment.file.ReadAt(buf[:], pos); err != nil {
		return 0, 0, 0, fmt.Errorf("read batch meta at %d: %w", pos, err)
	}
	firstOffset = binary.BigEndian.Uint64(buf[0:8])
	batchLength := binary.BigEndian.Uint32(buf[8:12])
	lastOffsetDelta = binary.BigEndian.Uint32(buf[18:22])
	onDiskSize = 16 + int64(batchLength)
	return firstOffset, lastOffsetDelta, onDiskSize, nil
}

func (segment *Segment) IsFull() bool        { return segment.currentSize.Load() >= segment.maxSize }
func (segment *Segment) BaseOffset() uint64  { return segment.baseOffset }
func (segment *Segment) NextOffset() uint64  { return segment.nextOffset.Load() }
func (segment *Segment) Size() int64         { return segment.currentSize.Load() }
func (segment *Segment) Sync() error         { return segment.file.Sync() }
func (segment *Segment) Close() error        { return segment.file.Close() }
