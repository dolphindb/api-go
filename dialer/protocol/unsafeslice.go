// Package protocol contains functions for zero-copy casting between typed slices and byte slices.
package protocol

import (
	"reflect"
	"unsafe"
)

// Useful constants.
const (
	TwoUint64Size = 16
	Uint64Size    = 8
	Uint32Size    = 4
	Uint16Size    = 2
	Uint8Size     = 1
)

func newRawSliceHeader(sh *reflect.SliceHeader, b []byte, stride int) *reflect.SliceHeader {
	sh.Len = len(b) / stride
	sh.Cap = len(b) / stride
	sh.Data = (uintptr)(unsafe.Pointer(&b[0]))
	return sh
}

func newSliceHeaderFromBytes(b []byte, stride int) unsafe.Pointer {
	//nolint
	sh := &reflect.SliceHeader{}
	return unsafe.Pointer(newRawSliceHeader(sh, b, stride))
}

func newSliceHeader(p unsafe.Pointer, size int) unsafe.Pointer {
	//nolint
	return unsafe.Pointer(&reflect.SliceHeader{
		Len:  size,
		Cap:  size,
		Data: uintptr(p),
	})
}

// ByteSliceFromInt8Slice casts b to []byte.
func ByteSliceFromInt8Slice(b []int8) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint8Size))
}

// ByteSliceFromUint8Slice casts b to []byte.
func ByteSliceFromUint8Slice(b []uint8) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return b
}

// ByteSliceFromInt16Slice casts b to []byte.
func ByteSliceFromInt16Slice(b []int16) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint16Size))
}

// ByteSliceFromUint16Slice casts b to []byte.
func ByteSliceFromUint16Slice(b []uint16) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint16Size))
}

// ByteSliceFromInt32Slice casts b to []byte.
func ByteSliceFromInt32Slice(b []int32) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint32Size))
}

// ByteSliceFromUint32Slice casts b to []byte.
func ByteSliceFromUint32Slice(b []uint32) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint32Size))
}

// ByteSliceFromInt64Slice casts b to []byte.
func ByteSliceFromInt64Slice(b []int64) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint64Size))
}

// ByteSliceFromUint64Slice casts b to []byte.
func ByteSliceFromUint64Slice(b []uint64) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint64Size))
}

// ByteSliceFromFloat32Slice casts b to []byte.
func ByteSliceFromFloat32Slice(b []float32) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint32Size))
}

// ByteSliceFromFloat64Slice casts b to []byte.
func ByteSliceFromFloat64Slice(b []float64) []byte {
	if len(b) == 0 {
		return []byte{}
	}
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(&b[0]), len(b)*Uint64Size))
}

// Float32SliceFromByteSlice casts b to []byte.
func Float32SliceFromByteSlice(b []byte) []float32 {
	return *(*[]float32)(newSliceHeaderFromBytes(b, Uint32Size))
}

// Float64SliceFromByteSlice casts b to []byte.
func Float64SliceFromByteSlice(b []byte) []float64 {
	return *(*[]float64)(newSliceHeaderFromBytes(b, Uint64Size))
}

// Uint64SliceFromByteSlice casts b to []uint64.
func Uint64SliceFromByteSlice(b []byte) []uint64 {
	return *(*[]uint64)(newSliceHeaderFromBytes(b, Uint64Size))
}

// Int64SliceFromByteSlice casts b to  []int6.
func Int64SliceFromByteSlice(b []byte) []int64 {
	return *(*[]int64)(newSliceHeaderFromBytes(b, Uint64Size))
}

// Uint32SliceFromByteSlice casts b to []uint32.
func Uint32SliceFromByteSlice(b []byte) []uint32 {
	return *(*[]uint32)(newSliceHeaderFromBytes(b, Uint32Size))
}

// Int32SliceFromByteSlice casts b to []int32.
func Int32SliceFromByteSlice(b []byte) []int32 {
	return *(*[]int32)(newSliceHeaderFromBytes(b, Uint32Size))
}

// Uint16SliceFromByteSlice casts b to []uint16.
func Uint16SliceFromByteSlice(b []byte) []uint16 {
	return *(*[]uint16)(newSliceHeaderFromBytes(b, Uint16Size))
}

// Int16SliceFromByteSlice casts b to []int16.
func Int16SliceFromByteSlice(b []byte) []int16 {
	return *(*[]int16)(newSliceHeaderFromBytes(b, Uint16Size))
}

// Uint8SliceFromByteSlice casts b to  []uint8.
func Uint8SliceFromByteSlice(b []byte) []uint8 {
	return b
}

// Int8SliceFromByteSlice casts b to []int8.
func Int8SliceFromByteSlice(b []byte) []int8 {
	return *(*[]int8)(newSliceHeaderFromBytes(b, Uint8Size))
}

// ByteSliceFromString casts b to []byte.
func ByteSliceFromString(s string) []byte {
	h := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return *(*[]byte)(newSliceHeader(unsafe.Pointer(h.Data), len(s)*Uint8Size))
}

// StringFromByteSlice casts b to string.
func StringFromByteSlice(b []byte) string {
	//nolint
	h := &reflect.StringHeader{
		//nolint
		Data: uintptr(unsafe.Pointer(&b[0])),
		Len:  len(b),
	}
	return *(*string)(unsafe.Pointer(h))
}
