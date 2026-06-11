package protocol

import (
	"fmt"
	"hash/crc32"
)

/*
 * Kafka RecordBatch on-wire format (magic = 2, KIP-98).
 *
 * Offsets:
 *   0   baseOffset           int64  (8)
 *   8   batchLength          int32  (4) - bytes from offset 12 to end
 *  12   partitionLeaderEpoch int32  (4)
 *  16   magic                int8   (1) = 2
 *  17   crc                  uint32 (4) - CRC-32C of bytes 21..end
 *  21   attributes           int16  (2)
 *  23   lastOffsetDelta      int32  (4)
 *  27   baseTimestamp        int64  (8)
 *  35   maxTimestamp         int64  (8)
 *  43   producerId           int64  (8) = −1
 *  51   producerEpoch        int16  (2) = −1
 *  53   baseSequence         int32  (4) = −1
 *  57   recordCount          int32  (4)
 *  61   records...
 *
 * Total fixed header -> 61 bytes
 *
 * Each record uses varint encoding:
 *   length        varint
 *   attributes    int8
 *   timestampDelta varint
 *   offsetDelta   varint
 *   keyLength     varint (-1 = null)
 *   key           bytes
 *   valueLength   varint (-1 = null)
 *   value         bytes
 *   headerCount   varint (0, currently dont use record headers)
 */

const (
	recordBatchHeaderSize = 61

	recordBatchLengthOffset = 8
	recordBatchBodyOffset   = 21
	recordBatchPrefixSize   = 12

	recordBatchBodyHeaderSize = recordBatchHeaderSize - recordBatchBodyOffset // 40
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func DecodeRecordBatch(data []byte) ([]*Batch, error) {
	var batches []*Batch
	pos := 0
	for pos < len(data) {
		batch, consumed, err := decodeOneRecordBatch(data[pos:])
		if err != nil {
			return batches, fmt.Errorf("RecordBatch at byte %d: %w", pos, err)
		}
		batches = append(batches, batch)
		pos += consumed
	}
	return batches, nil
}

func decodeOneRecordBatch(data []byte) (*Batch, int, error) {
	if len(data) < 12 {
		return nil, 0, fmt.Errorf("too short for RecordBatch header: %d bytes", len(data))
	}

	d := NewDecoder(data)
	baseOffset := d.Int64()
	batchLength := d.Int32()

	totalSize := 12 + int(batchLength)
	if len(data) < totalSize {
		return nil, 0, fmt.Errorf("need %d bytes, have %d", totalSize, len(data))
	}

	_ = d.Int32() // partitionLeaderEpoch unused for now
	magic := d.Int8()
	if magic != 2 {
		return nil, 0, fmt.Errorf("unsupported magic byte: %d (only magic 2 supported)", magic)
	}

	storedCRC := d.Uint32()

	// crc covers bytes from offset 21 to end of batch
	bodyBytes := data[recordBatchBodyOffset:totalSize]
	if crc32.Checksum(bodyBytes, crc32cTable) != storedCRC {
		return nil, 0, fmt.Errorf("CRC-32C mismatch")
	}

	bd := NewDecoder(bodyBytes)
	attributes := bd.Int16()
	lastOffsetDelta := bd.Int32()
	baseTimestamp := bd.Int64()
	maxTimestamp := bd.Int64()

	// reserved for later
	_ = bd.Int64() // producerId
	_ = bd.Int16() // producerEpoch
	_ = bd.Int32() // baseSequence
	recordCount := bd.Int32()

	if bd.Error() != nil {
		return nil, 0, bd.Error()
	}

	messages := make([]Message, recordCount)
	for i := range recordCount {
		// these are unused but i knowingly discard the return, the decoder needs to move forward
		_ = bd.Varint() // record length
		_ = bd.Int8()   // attributes
		tsDelta := bd.Varint()
		_ = bd.Varint() // offsetDelta

		keyLen := bd.Varint()
		var key []byte
		if keyLen >= 0 {
			key = bd.RawBytes(int(keyLen))
		}

		valLen := bd.Varint()
		var val []byte
		if valLen >= 0 {
			val = bd.RawBytes(int(valLen))
		}

		// skip record headers
		headerCount := bd.Varint()
		for range headerCount {
			hkLen := bd.Varint()
			if hkLen >= 0 {
				bd.RawBytes(int(hkLen))
			}
			hvLen := bd.Varint()
			if hvLen >= 0 {
				bd.RawBytes(int(hvLen))
			}
		}

		if bd.Error() != nil {
			return nil, 0, fmt.Errorf("record %d: %w", i, bd.Error())
		}

		messages[i] = Message{
			Key:       key,
			Value:     val,
			Timestamp: baseTimestamp + tsDelta,
		}
	}

	batch := &Batch{
		FirstOffset:     uint64(baseOffset),
		Attributes:      uint16(attributes),
		FirstTimestamp:  baseTimestamp,
		MaxTimestamp:    maxTimestamp,
		LastOffsetDelta: uint32(lastOffsetDelta),
		Messages:        messages,
	}

	return batch, totalSize, nil
}

func EncodeRecordBatch(batch *Batch, leaderEpoch int32) []byte {
	records := NewEncoder(len(batch.Messages) * 64)
	for i, msg := range batch.Messages {
		rec := encodeRecord(&msg, i, batch.FirstTimestamp)
		records.PutRawBytes(rec)
	}

	body := NewEncoder(recordBatchBodyHeaderSize + records.Len())
	body.PutInt16(int16(batch.Attributes))      // attributes
	body.PutInt32(int32(batch.LastOffsetDelta)) // lastOffsetDelta
	body.PutInt64(batch.FirstTimestamp)         // baseTimestamp
	body.PutInt64(batch.MaxTimestamp)           // maxTimestamp
	body.PutInt64(-1)                           // producerId
	body.PutInt16(-1)                           // producerEpoch
	body.PutInt32(-1)                           // baseSequence
	body.PutInt32(int32(len(batch.Messages)))   // recordCount
	body.PutRawBytes(records.Bytes())

	bodyData := body.Bytes()
	crc := crc32.Checksum(bodyData, crc32cTable)

	// batchLength = partitionLeaderEpoch(4) + magic(1) + crc(4) + bodyLen
	batchLength := 4 + 1 + 4 + len(bodyData)

	out := NewEncoder(12 + batchLength)
	out.PutInt64(int64(batch.FirstOffset)) // baseOffset
	out.PutInt32(int32(batchLength))       // batchLength
	out.PutInt32(leaderEpoch)              // partitionLeaderEpoch
	out.PutInt8(2)                         // magic
	out.PutUint32(crc)                     // crc 32c
	out.PutRawBytes(bodyData)

	return out.Bytes()
}

// encodeRecord serializes a single record in varint format.
func encodeRecord(msg *Message, index int, baseTimestamp int64) []byte {
	// encode body first so we can measure its length
	body := NewEncoder(32 + len(msg.Key) + len(msg.Value))
	body.PutInt8(0)                               // attributes
	body.PutVarint(msg.Timestamp - baseTimestamp) // timestampDelta
	body.PutVarint(int64(index))                  // offsetDelta

	if msg.Key == nil {
		body.PutVarint(-1)
	} else {
		body.PutVarint(int64(len(msg.Key)))
		body.PutRawBytes(msg.Key)
	}

	if msg.Value == nil {
		body.PutVarint(-1)
	} else {
		body.PutVarint(int64(len(msg.Value)))
		body.PutRawBytes(msg.Value)
	}

	body.PutVarint(0) // no record headers

	// prepend length varint
	rec := NewEncoder(body.Len() + 10)
	rec.PutVarint(int64(body.Len()))
	rec.PutRawBytes(body.Bytes())
	return rec.Bytes()
}
