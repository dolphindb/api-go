package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsafeSlice(t *testing.T) {
	bs := ByteSliceFromInt8Slice([]int8{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 1, 2})

	i8 := Int8SliceFromByteSlice(bs)
	assert.Equal(t, i8, []int8{0, 1, 2})

	bs = ByteSliceFromUint8Slice([]uint8{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 1, 2})

	u8 := Uint8SliceFromByteSlice(bs)
	assert.Equal(t, u8, []uint8{0, 1, 2})

	bs = ByteSliceFromInt16Slice([]int16{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 0, 1, 0, 2, 0})

	i16 := Int16SliceFromByteSlice(bs)
	assert.Equal(t, i16, []int16{0, 1, 2})

	bs = ByteSliceFromUint16Slice([]uint16{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 0, 1, 0, 2, 0})

	u16 := Uint16SliceFromByteSlice(bs)
	assert.Equal(t, u16, []uint16{0, 1, 2})

	bs = ByteSliceFromInt32Slice([]int32{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})

	i32 := Int32SliceFromByteSlice(bs)
	assert.Equal(t, i32, []int32{0, 1, 2})

	bs = ByteSliceFromUint32Slice([]uint32{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0})

	u32 := Uint32SliceFromByteSlice(bs)
	assert.Equal(t, u32, []uint32{0, 1, 2})

	bs = ByteSliceFromInt64Slice([]int64{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})

	i64 := Int64SliceFromByteSlice(bs)
	assert.Equal(t, i64, []int64{0, 1, 2})

	bs = ByteSliceFromUint64Slice([]uint64{0, 1, 2})
	assert.Equal(t, bs, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0})

	u64 := Uint64SliceFromByteSlice(bs)
	assert.Equal(t, u64, []uint64{0, 1, 2})

	bs = ByteSliceFromString("test")
	assert.Equal(t, string(bs), "test")

	s := StringFromByteSlice(bs)
	assert.Equal(t, s, "test")
}
