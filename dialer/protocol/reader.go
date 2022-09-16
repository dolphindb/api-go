package protocol

import (
	"bufio"
	"io"
)

// Reader interface declares functions to read data from reader.
type Reader interface {
	// ReadCertainBytes reads exactly count bytes from reader.
	// It returns the number of bytes copied and an error if fewer bytes were read.
	// The error is EOF only if no bytes were read.
	// If an EOF happens after reading some but not all the bytes,
	ReadCertainBytes(count int) ([]byte, error)
	// ReadByte reads and returns a single byte.
	// If no byte is available, returns an error.
	ReadByte() (byte, error)
	// ReadBytes reads until the first occurrence of delim in the input,
	// returning a slice containing the data up to and including the delimiter.
	// If ReadBytes encounters an error before finding a delimiter,
	// it returns the data read before the error and the error itself (often io.EOF).
	// ReadBytes returns err != nil if and only if the returned data does not end in
	// delim.
	// For simple uses, a Scanner may be more convenient.
	ReadBytes(delim byte) ([]byte, error)

	// Read reads data into p.
	// It returns the number of bytes read into p.
	// The bytes are taken from at most one Read on the underlying Reader,
	// hence n may be less than len(p).
	// To read exactly len(p) bytes, use io.ReadFull(b, p).
	// At EOF, the count will be zero and err will be io.EOF.
	Read(buf []byte) (int, error)
}

type reader struct {
	r *bufio.Reader
}

// NewReader returns a reader instance which implement the Reader.
func NewReader(rd io.Reader) Reader {
	return &reader{r: bufio.NewReaderSize(rd, 8192)}
}

// ReadCertainBytes reads exactly count bytes from reader.
// It returns the number of bytes copied and an error if fewer bytes were read.
// The error is EOF only if no bytes were read.
// If an EOF happens after reading some but not all the bytes.
func (r *reader) ReadCertainBytes(count int) ([]byte, error) {
	buf := make([]byte, count)
	_, err := io.ReadFull(r.r, buf)
	return buf, err
}

// ReadByte reads and returns a single byte.
// If no byte is available, returns an error.
func (r *reader) ReadByte() (byte, error) {
	return r.r.ReadByte()
}

// ReadBytes reads until the first occurrence of delim in the input,
// returning a slice containing the data up to and including the delimiter.
// If ReadBytes encounters an error before finding a delimiter,
// it returns the data read before the error and the error itself (often io.EOF).
// ReadBytes returns err != nil if and only if the returned data does not end in
// delim.
// For simple uses, a Scanner may be more convenient.
func (r *reader) ReadBytes(delim byte) ([]byte, error) {
	res, err := r.r.ReadBytes(delim)
	if err != nil {
		return nil, err
	}

	return res[:len(res)-1], nil
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// At EOF, the count will be zero and err will be io.EOF.
func (r *reader) Read(buf []byte) (int, error) {
	return r.r.Read(buf)
}
