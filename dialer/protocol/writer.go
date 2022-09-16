package protocol

import (
	"bufio"
	"io"
)

// Writer declares functions to writer data into io.Writer.
type Writer struct {
	wr *bufio.Writer
}

// NewWriter inits a writer with io.Writer.
func NewWriter(wr io.Writer) *Writer {
	return &Writer{
		wr: bufio.NewWriterSize(wr, 8192),
	}
}

// Write writes the contents of p into the buffer.
// It returns the number of bytes written. If nn < len(p),
// it also returns an error explaining why the write is short.
func (w *Writer) Write(d []byte) error {
	_, err := w.wr.Write(d)
	return err
}

// WriteByte writes a single byte.
func (w *Writer) WriteByte(d byte) error {
	return w.wr.WriteByte(d)
}

// WriteString writes a string. It returns the number of bytes written.
// If the count is less than len(s), it also returns an error
// explaining why the write is short.
func (w *Writer) WriteString(d string) error {
	_, err := w.wr.WriteString(d)
	return err
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	return w.wr.Flush()
}
