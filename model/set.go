package model

import (
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Set is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Set.html for more details.
type Set struct {
	category *Category

	Vector *Vector
}

// NewSet returns an object of Set based on vector v.
// You can instantiate v by NewVector.
func NewSet(v *Vector) *Set {
	return &Set{
		category: &Category{
			DataForm: DfSet,
			DataType: v.GetDataType(),
		},
		Vector: v,
	}
}

// Rows returns the row num of the DataForm.
func (s *Set) Rows() int {
	return int(s.Vector.RowCount)
}

// GetDataForm returns the byte type of the DataForm.
func (s *Set) GetDataForm() DataFormByte {
	return DfSet
}

// GetDataType returns the byte type of the DataType.
func (s *Set) GetDataType() DataTypeByte {
	return s.category.DataType
}

// GetDataTypeString returns the string format of the DataType.
func (s *Set) GetDataTypeString() string {
	return GetDataTypeString(s.category.DataType)
}

// Render serializes the DataForm with bo and input it into w.
func (s *Set) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	if err := s.category.render(w); err != nil {
		return err
	}

	return s.Vector.Render(w, bo)
}

func (s *Set) String() string {
	if s.Vector == nil {
		return ""
	}

	val := s.Vector.formatString()

	return fmt.Sprintf("set<%s>[%d]([%s])", GetDataTypeString(s.Vector.GetDataType()),
		s.Vector.ColumnCount*s.Vector.RowCount, strings.Join(val, ", "))
}
