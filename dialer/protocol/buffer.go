package protocol

import (
	"bytes"
)

// Buffer helps to read blobs efficiently.
type Buffer struct {
	buf   *bytes.Buffer
	ind   int64
	l     int64
	count int

	r Reader
}

// NewBuffer inits a Buffer object.
func NewBuffer(count int, r Reader) *Buffer {
	return &Buffer{
		count: count,
		r:     r,
	}
}

func (b *Buffer) isEmpty() bool {
	return b.l <= b.ind
}

// ReadBlobs helps to read blobs.
func (b *Buffer) ReadBlobs(bo ByteOrder) ([][]byte, error) {
	res := make([][]byte, b.count)
	ind := 0
	for b.count > 0 {
		l, err := b.read(4)
		if err != nil {
			return nil, err
		}

		b.count--

		length := int(bo.Uint32(l))
		if length == 0 {
			continue
		}

		res[ind], err = b.read(length)
		if err != nil {
			return nil, err
		}

		ind++
	}

	return res, nil
}

func (b *Buffer) fill(count int) error {
	if count == 0 {
		count = 1
	}

	tmp, err := b.r.ReadCertainBytes(count)
	if err != nil {
		return err
	}

	b.buf = bytes.NewBuffer(tmp)
	b.ind = 0
	b.l = int64(count)
	return err
}

func (b *Buffer) read(length int) ([]byte, error) {
	if b.isEmpty() {
		err := b.fill(4 * b.count)
		if err != nil {
			return nil, err
		}
	}

	return b.copy(length)
}

func (b *Buffer) copy(count int) ([]byte, error) {
	res := b.buf.Next(count)
	lt := len(res)
	b.ind += int64(lt)
	if lt < count {
		err := b.fill(4 * b.count)
		if err != nil {
			return nil, err
		}

		tmp, err := b.copy(count - lt)
		if err != nil {
			return nil, err
		}
		res = append(res, tmp...)
	}

	return res, nil
}
