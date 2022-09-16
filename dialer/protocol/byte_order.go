package protocol

import "encoding/binary"

const (
	// BigEndianByte is the byte type of BigEndian.
	BigEndianByte byte = '0'
	// LittleEndianByte is the byte type of LittleEndian.
	LittleEndianByte byte = '1'
)

var (
	// BigEndian is the big-endian implementation of ByteOrder.
	BigEndian = &bigEndian{binary.BigEndian}
	// LittleEndian is the little-endian implementation of ByteOrder.
	LittleEndian = &littleEndian{binary.LittleEndian}

	byteOrderSet = map[byte]ByteOrder{
		BigEndianByte:    BigEndian,
		LittleEndianByte: LittleEndian,
	}
)

// ByteOrder interface declares functions about how to handle data.
type ByteOrder interface {
	binary.ByteOrder
}

type littleEndian struct {
	binary.ByteOrder
}

type bigEndian struct {
	binary.ByteOrder
}

// GetByteOrder returns the BigEndian or LittleEndian according to the b.
// '0' return BigEndian or '1' return LittleEndian.
func GetByteOrder(b byte) ByteOrder {
	return byteOrderSet[b]
}
