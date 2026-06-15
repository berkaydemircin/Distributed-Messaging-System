package server

import (
	"io"
	"net"
	"os"
)

type Response interface {
	BodySize() int64
	WriteBodyTo(w *ResponseWriter) error
}

type BytesResponse []byte

func (r BytesResponse) BodySize() int64                     { return int64(len(r)) }
func (r BytesResponse) WriteBodyTo(w *ResponseWriter) error { return w.WriteBytes(r) }

type ResponsePart interface {
	Len() int64
	WriteTo(w *ResponseWriter) error
}

type BytesPart []byte

func (p BytesPart) Len() int64                      { return int64(len(p)) }
func (p BytesPart) WriteTo(w *ResponseWriter) error { return w.WriteBytes(p) }

type FilePart struct {
	File   *os.File
	Offset int64
	Length int64
}

func (p FilePart) Len() int64 { return p.Length }
func (p FilePart) WriteTo(w *ResponseWriter) error {
	return w.WriteFileRange(p.File, p.Offset, p.Length)
}

type CompositeResponse struct {
	Parts []ResponsePart
	size  int64
}

func NewCompositeResponse(parts []ResponsePart) *CompositeResponse {
	var size int64
	for _, p := range parts {
		size += p.Len()
	}
	return &CompositeResponse{Parts: parts, size: size}
}
func (r *CompositeResponse) BodySize() int64 { return r.size }
func (r *CompositeResponse) WriteBodyTo(w *ResponseWriter) error {
	for _, p := range r.Parts {
		if err := p.WriteTo(w); err != nil {
			return err
		}
	}
	return nil
}

type ResponseWriter struct {
	conn net.Conn
	tcp  *net.TCPConn
}

func NewResponseWriter(conn net.Conn) *ResponseWriter {
	tcp, _ := conn.(*net.TCPConn)
	return &ResponseWriter{conn: conn, tcp: tcp}
}

func (w *ResponseWriter) WriteBytes(b []byte) error {
	for len(b) > 0 {
		n, err := w.conn.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}

func (w *ResponseWriter) WriteFileRange(f *os.File, off, n int64) error {
	if n <= 0 {
		return nil
	}
	if w.tcp == nil {
		_, err := io.CopyN(w.conn, io.NewSectionReader(f, off, n), n)
		return err
	}
	return sendFileRange(w.tcp, f, off, n)
}
