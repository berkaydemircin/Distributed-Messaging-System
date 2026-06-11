package protocol

import "encoding/binary"

type Encoder struct {
	b []byte
}

func NewEncoder(capacity int) *Encoder {
	return &Encoder{b: make([]byte, 0, capacity)}
}

func (e *Encoder) PutInt8(v int8)     { e.b = append(e.b, byte(v)) }
func (e *Encoder) PutInt16(v int16)   { e.b = binary.BigEndian.AppendUint16(e.b, uint16(v)) }
func (e *Encoder) PutInt32(v int32)   { e.b = binary.BigEndian.AppendUint32(e.b, uint32(v)) }
func (e *Encoder) PutInt64(v int64)   { e.b = binary.BigEndian.AppendUint64(e.b, uint64(v)) }
func (e *Encoder) PutUint32(v uint32) { e.b = binary.BigEndian.AppendUint32(e.b, v) }
func (e *Encoder) PutBool(v bool) {
	if v {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}

// var sized ints

func (e *Encoder) PutUvarint(v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	e.b = append(e.b, buf[:n]...)
}

// writes zigzag encoded int
func (e *Encoder) PutVarint(v int64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	e.b = append(e.b, buf[:n]...)
}

func (e *Encoder) PutString(s string) {
	e.PutInt16(int16(len(s)))
	e.b = append(e.b, s...)
}

func (e *Encoder) PutNullableString(s *string) {
	if s == nil {
		e.PutInt16(-1)
		return
	}
	e.PutString(*s)
}

func (e *Encoder) PutCompactString(s string) {
	e.PutUvarint(uint64(len(s)) + 1)
	e.b = append(e.b, s...)
}

func (e *Encoder) PutCompactNullableString(s *string) {
	if s == nil {
		e.PutUvarint(0)
		return
	}
	e.PutCompactString(*s)
}

func (e *Encoder) PutBytes(b []byte) {
	e.PutInt32(int32(len(b)))
	e.b = append(e.b, b...)
}

func (e *Encoder) PutNullableBytes(b []byte) {
	if b == nil {
		e.PutInt32(-1)
		return
	}
	e.PutBytes(b)
}

func (e *Encoder) PutCompactBytes(b []byte) {
	e.PutUvarint(uint64(len(b)) + 1)
	e.b = append(e.b, b...)
}

func (e *Encoder) PutRawBytes(b []byte) { e.b = append(e.b, b...) }

func (e *Encoder) PutArrayLen(n int32) { e.PutInt32(n) }

func (e *Encoder) PutCompactArrayLen(n int) { e.PutUvarint(uint64(n) + 1) }

func (e *Encoder) PutTaggedFields() { e.PutUvarint(0) }

func (e *Encoder) ReserveInt32() int {
	pos := len(e.b)
	e.b = append(e.b, 0, 0, 0, 0)
	return pos
}

func (e *Encoder) FillInt32(pos int, v int32) {
	binary.BigEndian.PutUint32(e.b[pos:], uint32(v))
}

func (e *Encoder) Bytes() []byte { return e.b }
func (e *Encoder) Len() int      { return len(e.b) }
func (e *Encoder) Reset()        { e.b = e.b[:0] }
