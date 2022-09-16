package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

func TestIo(t *testing.T) {
	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	r := protocol.NewReader(by)
	bo := protocol.LittleEndian

	dt, err := NewDataType(DtString, "io test")
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x69, 0x6f, 0x20, 0x74, 0x65, 0x73, 0x74, 0x0})

	pDt, err := read(r, DtString, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtFloat, float32(1.0))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x80, 0x3f})

	pDt, err = read(r, DtFloat, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtDouble, float64(1))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	pDt, err = read(r, DtDouble, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtDuration, "10H")
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0})

	pDt, err = read(r, DtDuration, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtComplex, [2]float64{1, 1})
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	pDt, err = read(r, DtComplex, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtBlob, []byte{1, 2, 3, 4, 5})
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x5, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5})

	pDt, err = read(r, DtBlob, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtShort, int16(10))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0})

	pDt, err = read(r, DtShort, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	s := NewScalar(dt)
	dt, err = NewDataType(DtAny, s)
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x3, 0x0, 0xa, 0x0})

	l, err := readList(r, DtAny, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	dt, err = NewDataType(DtBool, byte(1))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x1})

	l, err = readList(r, DtBool, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	dt, err = NewDataType(DtShort, int16(10))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0})

	l, err = readList(r, DtShort, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0})

	l, err = readList(r, DtShort, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), int16(2560))

	dt, err = NewDataType(DtLong, int64(10))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})

	l, err = readList(r, DtLong, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), int64(10))

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})

	pDt, err = read(r, DtLong, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})

	l, err = readList(r, DtLong, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), int64(720575940379279360))

	dt, err = NewDataType(DtInt, int32(10))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0})

	l, err = readList(r, DtInt, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), int32(167772160))

	dt, err = NewDataType(DtInt128, "e1671797c52e15f763380b45e841ec32")
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x32, 0xec, 0x41, 0xe8, 0x45, 0xb, 0x38, 0x63, 0xf7, 0x15, 0x2e, 0xc5, 0x97, 0x17, 0x67, 0xe1})

	pDt, err = read(r, DtInt128, bo)
	assert.Nil(t, err)
	assert.Equal(t, pDt.Value(), dt.Value())

	dt, err = NewDataType(DtUUID, "e5eca940-5b99-45d0-bf1c-620f6b1b9d5b")
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x5b, 0x9d, 0x1b, 0x6b, 0xf, 0x62, 0x1c, 0xbf, 0xd0, 0x45, 0x99, 0x5b, 0x40, 0xa9, 0xec, 0xe5})

	l, err = readList(r, DtUUID, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x5b, 0x9d, 0x1b, 0x6b, 0xf, 0x62, 0x1c, 0xbf, 0xd0, 0x45, 0x99, 0x5b, 0x40, 0xa9, 0xec, 0xe5})

	l, err = readList(r, DtUUID, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), "40a9ece5-995b-d045-1cbf-5b9d1b6b0f620000")

	dt, err = NewDataType(DtFloat, float32(1))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x80, 0x3f})

	l, err = readList(r, DtFloat, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x80, 0x3f})

	l, err = readList(r, DtFloat, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), float32(4.6006e-41))

	dt, err = NewDataType(DtDouble, float64(1))
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	l, err = readList(r, DtDouble, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	l, err = readList(r, DtDouble, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), 3.03865e-319)

	dt, err = NewDataType(DtDuration, "10H")
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0})

	l, err = readList(r, DtDuration, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0})

	l, err = readList(r, DtDuration, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), "167772160")

	dt, err = NewDataType(DtComplex, [2]float64{1, 1})
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	l, err = readList(r, DtComplex, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	l, err = readList(r, DtComplex, protocol.BigEndian, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), "0.00000+0.00000i")

	dt, err = NewDataType(DtBlob, []byte{1, 2, 3, 4})
	assert.Nil(t, err)

	err = dt.Render(w, bo)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x4, 0x0, 0x0, 0x0, 0x1, 0x2, 0x3, 0x4})

	l, err = readList(r, DtBlob, bo, 1)
	assert.Nil(t, err)
	assert.Equal(t, l.Get(0).Value(), dt.Value())
}
