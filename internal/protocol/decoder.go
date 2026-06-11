package protocol

import (
	"encoding/binary"
	"fmt"
)

type Decoder struct {
	buf []byte
	pos int
	err error
}

func NewDecoder(buf []byte) *Decoder { return &Decoder{buf: buf} }

func (d *Decoder) Error() error   { return d.err }
func (d *Decoder) Remaining() int { return len(d.buf) - d.pos }
func (d *Decoder) Offset() int    { return d.pos }

func (d *Decoder) require(n int) bool {
	if d.err != nil {
		return false
	}
	if d.pos+n > len(d.buf) {
		d.err = fmt.Errorf("short buffer: need %d bytes at offset %d, have %d",
			n, d.pos, len(d.buf))
		return false
	}
	return true
}

func (d *Decoder) Int8() int8 {
	if !d.require(1) {
		return 0
	}
	v := int8(d.buf[d.pos])
	d.pos++
	return v
}

func (d *Decoder) Int16() int16 {
	if !d.require(2) {
		return 0
	}
	v := int16(binary.BigEndian.Uint16(d.buf[d.pos:]))
	d.pos += 2
	return v
}

func (d *Decoder) Int32() int32 {
	if !d.require(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(d.buf[d.pos:]))
	d.pos += 4
	return v
}

func (d *Decoder) Int64() int64 {
	if !d.require(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return v
}

func (d *Decoder) Uint32() uint32 {
	if !d.require(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf[d.pos:])
	d.pos += 4
	return v
}

func (d *Decoder) Bool() bool { return d.Int8() != 0 }

func (d *Decoder) Uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.buf[d.pos:])
	if n <= 0 {
		d.err = fmt.Errorf("uvarint decode failed at offset %d", d.pos)
		return 0
	}
	d.pos += n
	return v
}

func (d *Decoder) Varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.buf[d.pos:])
	if n <= 0 {
		d.err = fmt.Errorf("varint decode failed at offset %d", d.pos)
		return 0
	}
	d.pos += n
	return v
}

func (d *Decoder) String() string {
	n := int(d.Int16())
	if d.err != nil || n < 0 {
		return ""
	}
	if !d.require(n) {
		return ""
	}
	s := string(d.buf[d.pos : d.pos+n])
	d.pos += n
	return s
}

func (d *Decoder) NullableString() *string {
	n := d.Int16()
	if d.err != nil {
		return nil
	}
	if n < 0 {
		return nil
	}
	length := int(n)
	if !d.require(length) {
		return nil
	}
	s := string(d.buf[d.pos : d.pos+length])
	d.pos += length
	return &s
}

func (d *Decoder) CompactString() string {
	raw := d.Uvarint()
	if d.err != nil {
		return ""
	}
	if raw == 0 {
		return ""
	}
	length := int(raw) - 1
	if !d.require(length) {
		return ""
	}
	s := string(d.buf[d.pos : d.pos+length])
	d.pos += length
	return s
}

func (d *Decoder) CompactNullableString() *string {
	raw := d.Uvarint()
	if d.err != nil {
		return nil
	}
	if raw == 0 {
		return nil
	}
	length := int(raw) - 1
	if !d.require(length) {
		return nil
	}
	s := string(d.buf[d.pos : d.pos+length])
	d.pos += length
	return &s
}

func (d *Decoder) NullableBytes() []byte {
	n := d.Int32()
	if d.err != nil {
		return nil
	}
	if n < 0 {
		return nil
	}
	length := int(n)
	if !d.require(length) {
		return nil
	}
	b := make([]byte, length)
	copy(b, d.buf[d.pos:d.pos+length])
	d.pos += length
	return b
}

func (d *Decoder) RawBytes(n int) []byte {
	if !d.require(n) {
		return nil
	}
	b := d.buf[d.pos : d.pos+n : d.pos+n]
	d.pos += n
	return b
}

func (d *Decoder) ArrayLen() int32 { return d.Int32() }

func (d *Decoder) CompactArrayLen() int {
	v := d.Uvarint()
	if v == 0 {
		return -1
	}
	return int(v) - 1
}

func (d *Decoder) DiscardTaggedFields() {
	if d.err != nil {
		return
	}
	numFields := d.Uvarint()
	for range numFields {
		_ = d.Uvarint() // tag
		length := int(d.Uvarint())
		if !d.require(length) {
			return
		}
		d.pos += length
	}
}
