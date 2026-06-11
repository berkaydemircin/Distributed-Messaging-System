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
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

const (
	// Layout (all big endian):
	//   [0:8]   baseOffset              int64
	//   [8:12]  batchLength             int32  - byte count from [12:end]
	//   [12:16] partitionLeaderEpoch    int32  - written by broker on append
	//   [16]    magic                   int8   = 2
	//   [17:21] crc                     uint32 - CRC32C of bytes [21:end]
	//   [21:23] attributes              int16
	//   [23:27] lastOffsetDelta         int32
	//   [27:35] firstTimestamp          int64
	//   [35:43] maxTimestamp            int64
	//   [43:51] producerId              int64  = −1 for non-transactional
	//   [51:53] producerEpoch           int16  = −1
	//   [53:57] baseSequence            int32  = −1
	//   [57:61] recordCount             int32
	//   [61:]   records
	recordBatchOverhead = 61
	recoverHeaderRead   = recordBatchOverhead
)

type Segment struct {
	file        *os.File
	baseOffset  uint64
	nextOffset  atomic.Uint64
	currentSize atomic.Int64
	maxSize     int64
}

// NewSegment opens or creates the segment file at baseOffset and recovers
// in-memory state by scanning existing data.
func NewSegment(dir string, baseOffset uint64, maxSize int64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	s := &Segment{file: file, baseOffset: baseOffset, maxSize: maxSize}
	s.nextOffset.Store(baseOffset)
	s.currentSize.Store(0)

	if err := s.recover(); err != nil {
		file.Close()
		return nil, fmt.Errorf("recover segment: %w", err)
	}
	return s, nil
}

func (s *Segment) recover() error {
	info, err := s.file.Stat()
	if err != nil {
		return err
	}

	fileSize := info.Size()
	if fileSize == 0 {
		return nil
	}

	header := make([]byte, recoverHeaderRead)
	var pos int64

	for pos < fileSize {
		n, err := s.file.ReadAt(header, pos)
		if err != nil {
			if err == io.EOF || n < recoverHeaderRead {
				if err := s.file.Truncate(pos); err != nil {
					return fmt.Errorf("truncate partial header at %d: %w", pos, err)
				}
				break
			}
			return fmt.Errorf("read header at %d: %w", pos, err)
		}

		// batchLength covers bytes [12:end], so total = 12 + batchLength.
		batchLength := int64(binary.BigEndian.Uint32(header[8:12]))
		totalSize := 12 + batchLength

		if pos+totalSize > fileSize {
			if err := s.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate partial batch at %d: %w", pos, err)
			}
			break
		}

		// magic
		if header[16] != 2 {
			if err := s.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate bad magic at %d: %w", pos, err)
			}
			break
		}

		// CRC32C covers bytes [21:totalSize] (from attributes to end)
		storedCRC := binary.BigEndian.Uint32(header[17:21])
		crcBody := make([]byte, totalSize-21)
		if _, err := s.file.ReadAt(crcBody, pos+21); err != nil {
			if err := s.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate unreadable crc body at %d: %w", pos, err)
			}
			break
		}
		if crc32.Checksum(crcBody, crc32cTable) != storedCRC {
			if err := s.file.Truncate(pos); err != nil {
				return fmt.Errorf("truncate corrupt batch at %d: %w", pos, err)
			}
			break
		}

		// lastOffsetDelta at [23:27]
		lastOffsetDelta := binary.BigEndian.Uint32(header[23:27])
		s.nextOffset.Add(uint64(lastOffsetDelta) + 1)
		pos += totalSize
	}

	s.currentSize.Store(pos)

	if s.nextOffset.Load() == s.baseOffset && pos > 0 {
		return fmt.Errorf("corrupt segment: non-empty file but no valid batches")
	}
	return nil
}

// as a personal note, the concept of var size ints, zigzag encode and leb128 are genius
func varintSize(v int64) int {
	u := uint64((v << 1) ^ (v >> 63)) // zigzag encode
	size := 1
	for u >= 0x80 { // LEB128
		u >>= 7
		size++
	}
	return size
}

func encodedBatchSize(batch *protocol.Batch) int {
	total := recordBatchOverhead
	for i, msg := range batch.Messages {
		tsDelta := msg.Timestamp - batch.FirstTimestamp
		offsetDelta := int64(i)
		bodySize := 1 + // attributes (int8)
			varintSize(tsDelta) +
			varintSize(offsetDelta) +
			1 + len(msg.Key) // keyLen varint + key bytes, null is handled below
		if msg.Key == nil {
			bodySize = bodySize - 1 + varintSize(-1) // replace 1-byte assumption with actual -1 varint
		} else {
			bodySize += varintSize(int64(len(msg.Key))) - 1 // replace 1 byte assumption if msg is bigger
		}

		// value
		if msg.Value == nil {
			bodySize += varintSize(-1)
		} else {
			bodySize += varintSize(int64(len(msg.Value))) + len(msg.Value)
		}
		bodySize += 1 // headers count varint (0)
		total += varintSize(int64(bodySize)) + bodySize
	}
	return total
}

func encodeBatch(batch *protocol.Batch, leaderEpoch int32) []byte {
	recBuf := make([]byte, 0, len(batch.Messages)*64)
	for i, msg := range batch.Messages {
		recBuf = appendRecord(recBuf, &msg, int64(i), batch.FirstTimestamp)
	}

	crcBodySize := (recordBatchOverhead - 21) + len(recBuf)
	crcBody := make([]byte, 0, crcBodySize)

	crcBody = binary.BigEndian.AppendUint16(crcBody, batch.Attributes)             // [21:23] attributes
	crcBody = binary.BigEndian.AppendUint32(crcBody, batch.LastOffsetDelta)        // [23:27] lastOffsetDelta
	crcBody = binary.BigEndian.AppendUint64(crcBody, uint64(batch.FirstTimestamp)) // [27:35] firstTimestamp
	crcBody = binary.BigEndian.AppendUint64(crcBody, uint64(batch.MaxTimestamp))   // [35:43] maxTimestamp
	crcBody = binary.BigEndian.AppendUint64(crcBody, uint64(0xFFFFFFFFFFFFFFFF))   // [43:51] producerId   = -1
	crcBody = binary.BigEndian.AppendUint16(crcBody, 0xFFFF)                       // [51:53] producerEpoch = -1
	crcBody = binary.BigEndian.AppendUint32(crcBody, 0xFFFFFFFF)                   // [53:57] baseSequence = -1
	crcBody = binary.BigEndian.AppendUint32(crcBody, uint32(len(batch.Messages)))  // [57:61] recordCount
	crcBody = append(crcBody, recBuf...)

	checksum := crc32.Checksum(crcBody, crc32cTable)

	// batchLength covers [12:end] = 4(epoch) + 1(magic) + 4(crc) + len(crcBody)
	batchLength := 4 + 1 + 4 + len(crcBody)
	totalSize := 12 + batchLength

	buf := make([]byte, totalSize)
	binary.BigEndian.PutUint64(buf[0:8], batch.FirstOffset)     // [0:8] baseOffset
	binary.BigEndian.PutUint32(buf[8:12], uint32(batchLength))  // [8:12] batchLength
	binary.BigEndian.PutUint32(buf[12:16], uint32(leaderEpoch)) // [12:16] partitionLeaderEpoch
	buf[16] = 2                                                 // [16] magic = 2
	binary.BigEndian.PutUint32(buf[17:21], checksum)            // [17:21] CRC32C
	copy(buf[21:], crcBody)

	return buf
}

func appendRecord(dst []byte, msg *protocol.Message, offsetDelta int64, baseTimestamp int64) []byte {
	tsDelta := msg.Timestamp - baseTimestamp

	body := make([]byte, 0, 32+len(msg.Key)+len(msg.Value))
	body = append(body, 0) // attribujtes
	body = binary.AppendVarint(body, tsDelta)
	body = binary.AppendVarint(body, offsetDelta)

	if msg.Key == nil {
		body = binary.AppendVarint(body, -1)
	} else {
		body = binary.AppendVarint(body, int64(len(msg.Key)))
		body = append(body, msg.Key...)
	}

	if msg.Value == nil {
		body = binary.AppendVarint(body, -1)
	} else {
		body = binary.AppendVarint(body, int64(len(msg.Value)))
		body = append(body, msg.Value...)
	}

	body = binary.AppendVarint(body, 0)

	dst = binary.AppendVarint(dst, int64(len(body)))
	dst = append(dst, body...)
	return dst
}

func decodeBatch(data []byte) (*protocol.Batch, error) {
	if len(data) < recordBatchOverhead {
		return nil, fmt.Errorf("decodeBatch: too short: %d bytes", len(data))
	}

	baseOffset := binary.BigEndian.Uint64(data[0:8])
	batchLength := int64(binary.BigEndian.Uint32(data[8:12]))
	totalSize := 12 + batchLength

	if int64(len(data)) < totalSize {
		return nil, fmt.Errorf("decodeBatch: data truncated: have %d, need %d", len(data), totalSize)
	}

	if data[16] != 2 {
		return nil, fmt.Errorf("decodeBatch: unsupported magic %d", data[16])
	}

	// crc check crccheck
	storedCRC := binary.BigEndian.Uint32(data[17:21])
	if crc32.Checksum(data[21:totalSize], crc32cTable) != storedCRC {
		return nil, fmt.Errorf("decodeBatch: CRC32C mismatch")
	}

	batch := &protocol.Batch{}
	batch.FirstOffset = baseOffset
	batch.Attributes = binary.BigEndian.Uint16(data[21:23])
	batch.LastOffsetDelta = binary.BigEndian.Uint32(data[23:27])
	batch.FirstTimestamp = int64(binary.BigEndian.Uint64(data[27:35]))
	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(data[35:43]))
	// producerId [43:51], producerEpoch [51:53], baseSequence [53:57] - skip
	recordCount := int(binary.BigEndian.Uint32(data[57:61]))

	batch.Messages = make([]protocol.Message, recordCount)
	pos := 61 // records start at byte 61

	for i := range recordCount {
		if pos >= int(totalSize) {
			return nil, fmt.Errorf("decodeBatch: record %d: unexpected end of data", i)
		}

		recLen, n := binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("decodeBatch: record %d: bad length varint", i)
		}
		pos += n

		if pos+int(recLen) > int(totalSize) {
			return nil, fmt.Errorf("decodeBatch: record %d: length %d exceeds batch", i, recLen)
		}

		if pos >= int(totalSize) {
			return nil, fmt.Errorf("decodeBatch: record %d: truncated attributes", i)
		}
		pos++ // skip attributes

		// timestampDelta
		tsDelta, n := binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("decodeBatch: record %d: bad timestampDelta varint", i)
		}
		pos += n

		_, n = binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("decodeBatch: record %d: bad offsetDelta varint", i)
		}
		pos += n

		// key
		keyLen, n := binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("decodeBatch: record %d: bad keyLen varint", i)
		}
		pos += n
		var key []byte
		if keyLen >= 0 {
			if pos+int(keyLen) > int(totalSize) {
				return nil, fmt.Errorf("decodeBatch: record %d: key truncated", i)
			}
			key = data[pos : pos+int(keyLen) : pos+int(keyLen)]
			pos += int(keyLen)
		}

		// value
		valLen, n := binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("decodeBatch: record %d: bad valLen varint", i)
		}
		pos += n
		var val []byte
		if valLen >= 0 {
			if pos+int(valLen) > int(totalSize) {
				return nil, fmt.Errorf("decodeBatch: record %d: value truncated", i)
			}
			val = data[pos : pos+int(valLen) : pos+int(valLen)]
			pos += int(valLen)
		}

		// headers count - skip headers
		hdrCount, n := binary.Varint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("decodeBatch: record %d: bad headerCount varint", i)
		}
		pos += n
		for h := int64(0); h < hdrCount; h++ {
			hkLen, n := binary.Varint(data[pos:])
			if n <= 0 {
				return nil, fmt.Errorf("decodeBatch: record %d header %d: bad key len", i, h)
			}
			pos += n
			if hkLen > 0 {
				pos += int(hkLen)
			}
			hvLen, n := binary.Varint(data[pos:])
			if n <= 0 {
				return nil, fmt.Errorf("decodeBatch: record %d header %d: bad value len", i, h)
			}
			pos += n
			if hvLen > 0 {
				pos += int(hvLen)
			}
		}

		batch.Messages[i] = protocol.Message{
			Key:       key,
			Value:     val,
			Timestamp: batch.FirstTimestamp + tsDelta,
		}
	}

	return batch, nil
}

func (s *Segment) AppendBatch(batch *protocol.Batch) (int64, error) {
	if len(batch.Messages) == 0 {
		return 0, fmt.Errorf("AppendBatch: empty batch")
	}

	totalSize := encodedBatchSize(batch)
	if s.currentSize.Load()+int64(totalSize) > s.maxSize {
		return 0, ErrSegmentFull
	}

	batch.FirstOffset = s.nextOffset.Load()
	batch.LastOffsetDelta = uint32(len(batch.Messages) - 1)

	// leaderEpoch = 0 for the internal path (no leader context)
	buf := encodeBatch(batch, 0)
	pos := s.currentSize.Load()

	if _, err := s.file.WriteAt(buf, pos); err != nil {
		return 0, fmt.Errorf("AppendBatch: write at pos %d: %w", pos, err)
	}

	s.nextOffset.Add(uint64(len(batch.Messages)))
	s.currentSize.Add(int64(len(buf)))
	return pos, nil
}

func (s *Segment) AppendRawBatch(data []byte, leaderEpoch int32) (baseOffset uint64, pos int64, lastOffsetDelta uint32, err error) {
	if len(data) < recordBatchOverhead {
		return 0, 0, 0, fmt.Errorf("AppendRawBatch: too short (%d bytes, need ≥%d)", len(data), recordBatchOverhead)
	}

	batchLength := int64(binary.BigEndian.Uint32(data[8:12]))
	totalSize := 12 + batchLength

	if int64(len(data)) < totalSize {
		return 0, 0, 0, fmt.Errorf("AppendRawBatch: declared batchLength=%d needs %d bytes, have %d",
			batchLength, totalSize, len(data))
	}
	if int64(len(data)) != totalSize {
		return 0, 0, 0, fmt.Errorf("AppendRawBatch: multiple RecordBatches in one records field are not supported: first batch needs %d bytes, have %d",
			totalSize, len(data))
	}
	if data[16] != 2 {
		return 0, 0, 0, fmt.Errorf("AppendRawBatch: unsupported magic %d", data[16])
	}

	storedCRC := binary.BigEndian.Uint32(data[17:21])
	if crc32.Checksum(data[21:totalSize], crc32cTable) != storedCRC {
		return 0, 0, 0, fmt.Errorf("AppendRawBatch: CRC32C mismatch")
	}

	pos = s.currentSize.Load()
	if pos+totalSize > s.maxSize {
		return 0, 0, 0, ErrSegmentFull
	}

	// TODO copying for now so i dont mutate the callers network buffer but take 2nd look
	buf := make([]byte, totalSize)
	copy(buf, data[:totalSize])

	assignedOffset := s.nextOffset.Load()
	binary.BigEndian.PutUint64(buf[0:8], assignedOffset)        // baseOffset - before CRC region
	binary.BigEndian.PutUint32(buf[12:16], uint32(leaderEpoch)) // leaderEpoch - before CRC region

	if _, err := s.file.WriteAt(buf, pos); err != nil {
		return 0, 0, 0, fmt.Errorf("AppendRawBatch: write at pos %d: %w", pos, err)
	}

	lod := binary.BigEndian.Uint32(buf[23:27])
	s.nextOffset.Add(uint64(lod) + 1)
	s.currentSize.Add(totalSize)

	return assignedOffset, pos, lod, nil
}

// used by internal/test read path
func (s *Segment) ReadBatchAt(pos int64) (*protocol.Batch, error) {
	var prefix [12]byte
	if _, err := s.file.ReadAt(prefix[:], pos); err != nil {
		return nil, fmt.Errorf("ReadBatchAt: read prefix at %d: %w", pos, err)
	}
	batchLength := int64(binary.BigEndian.Uint32(prefix[8:12]))
	totalSize := 12 + batchLength

	buf := make([]byte, totalSize)
	if _, err := s.file.ReadAt(buf, pos); err != nil {
		return nil, fmt.Errorf("ReadBatchAt: read batch at %d: %w", pos, err)
	}
	return decodeBatch(buf)
}

func (s *Segment) ReadBatchMetaAt(pos int64) (firstOffset uint64, lastOffsetDelta uint32, onDiskSize int64, err error) {
	var buf [27]byte
	if _, err = s.file.ReadAt(buf[:], pos); err != nil {
		return 0, 0, 0, fmt.Errorf("ReadBatchMetaAt: read at %d: %w", pos, err)
	}
	firstOffset = binary.BigEndian.Uint64(buf[0:8])
	batchLength := int64(binary.BigEndian.Uint32(buf[8:12]))
	lastOffsetDelta = binary.BigEndian.Uint32(buf[23:27])
	onDiskSize = 12 + batchLength // batchLength covers [12:end]
	return firstOffset, lastOffsetDelta, onDiskSize, nil
}

func (s *Segment) ReadRawBatchAt(pos int64, maxBytes int32) ([]byte, uint64, error) {
	segSize := s.currentSize.Load()
	if pos >= segSize {
		return nil, 0, fmt.Errorf("ReadRawBatchAt: pos %d at or beyond segment size %d", pos, segSize)
	}

	var result []byte
	var fetchedUpTo uint64
	remaining := int64(maxBytes)
	first := true

	for pos < segSize {
		firstOffset, lastOffsetDelta, totalSize, err := s.ReadBatchMetaAt(pos)
		if err != nil {
			return nil, 0, fmt.Errorf("ReadRawBatchAt: read meta at pos %d: %w", pos, err)
		}

		if pos+totalSize > segSize {
			break // partial write at segment tail
		}
		if !first && totalSize > remaining {
			break // budget exhausted
		}

		buf := make([]byte, totalSize)
		if _, err := s.file.ReadAt(buf, pos); err != nil {
			return nil, 0, fmt.Errorf("ReadRawBatchAt: read batch at pos %d: %w", pos, err)
		}
		result = append(result, buf...)
		fetchedUpTo = firstOffset + uint64(lastOffsetDelta) + 1

		remaining -= totalSize
		pos += totalSize
		first = false

		if remaining <= 0 {
			break
		}
	}

	if len(result) == 0 {
		return nil, 0, fmt.Errorf("ReadRawBatchAt: no complete batch at pos %d", pos)
	}

	return result, fetchedUpTo, nil
}

func (s *Segment) IsFull() bool       { return s.currentSize.Load() >= s.maxSize }
func (s *Segment) BaseOffset() uint64 { return s.baseOffset }
func (s *Segment) NextOffset() uint64 { return s.nextOffset.Load() }
func (s *Segment) Size() int64        { return s.currentSize.Load() }
func (s *Segment) Sync() error        { return s.file.Sync() }
func (s *Segment) Close() error       { return s.file.Close() }
