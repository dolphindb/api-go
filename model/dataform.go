package model

import (
	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/errors"
)

// DataForm interface declares functions to handle DataForm data.
type DataForm interface {
	// Render serializes the DataForm with bo and input it into w
	Render(w *protocol.Writer, bo protocol.ByteOrder) error

	// GetDataForm returns the byte type of the DataForm
	GetDataForm() DataFormByte
	// GetDataFormString returns the string format of the DataForm
	GetDataFormString() string
	// GetDataType returns the byte type of the DataType
	GetDataType() DataTypeByte
	// GetDataTypeString returns the string format of the DataType
	GetDataTypeString() string

	// String returns the string of the DataForm
	String() string
	// Rows returns the row num of the DataForm
	Rows() int
}

// Category stores the DataFormByte and the DataTypeByte of a DataForm.
type Category struct {
	DataForm DataFormByte
	DataType DataTypeByte
}

func newCategory(dataForm, datatype byte) *Category {
	return &Category{
		DataForm: DataFormByte(dataForm),
		DataType: DataTypeByte(datatype),
	}
}

func (cg *Category) render(w *protocol.Writer) error {
	return w.Write(protocol.ByteSliceFromInt16Slice([]int16{int16(cg.DataForm) << 8 + int16(cg.DataType)}))
}

func parseCategory(r protocol.Reader, bo protocol.ByteOrder) (*Category, error) {
	c, err := readShort(r, bo)
	if err != nil {
		return nil, errors.ReadDataTypeAndDataFormError(err.Error())
	}
	return newCategory(byte(c >> 8), byte(c << 8 >> 8)), nil
}

// ParseDataForm parses the raw bytes in r with bo and return a DataForm object.
func ParseDataForm(r protocol.Reader, bo protocol.ByteOrder) (DataForm, error) {
	c, err := parseCategory(r, bo)
	if err != nil {
		return nil, err
	}

	switch c.DataForm {
	case DfScalar:
		return parseScalar(r, bo, c)
	case DfTable:
		return parseTable(r, bo, c)
	case DfVector:
		return parseVector(r, bo, c)
	case DfPair:
		return parsePair(r, bo, c)
	case DfMatrix:
		return parseMatrix(r, bo, c)
	case DfSet:
		return parseSet(r, bo, c)
	case DfDictionary:
		return parseDictionary(r, bo, c)
	case DfChart:
		return parseChart(r, bo, c)
	}

	return nil, err
}
