package model

import (
	"fmt"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Scalar is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Scalar.html for more details.
type Scalar struct {
	category *Category

	DataType
}

// NewScalar returns an object of scalar with d.
// You can instantiate the d by NewDataType.
func NewScalar(d DataType) *Scalar {
	return &Scalar{
		category: &Category{
			DataForm: DfScalar,
			DataType: d.DataType(),
		},
		DataType: d,
	}
}

// Rows returns the row num of the DataForm.
func (s *Scalar) Rows() int {
	return 1
}

// GetDataForm returns the byte type of the DataForm.
func (s *Scalar) GetDataForm() DataFormByte {
	return DfScalar
}

// GetDataType returns the byte type of the DataType.
func (s *Scalar) GetDataType() DataTypeByte {
	return s.category.DataType
}

// GetDataTypeString returns the string format of the DataType.
func (s *Scalar) GetDataTypeString() string {
	return GetDataTypeString(s.category.DataType)
}

// SetNull sets the value of scalar to null.
func (s *Scalar) SetNull() {
	s.DataType.SetNull()
}

// IsNull checks whether the value of scalar is null.
func (s *Scalar) IsNull() bool {
	return s.DataType.IsNull()
}

// Render serializes the DataForm with bo and input it into w.
func (s *Scalar) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	if err := s.category.render(w); err != nil {
		return err
	}

	return s.DataType.Render(w, bo)
}

func (s *Scalar) String() string {
	val := s.DataType.String()
	return fmt.Sprintf("%s(%s)", GetDataTypeString(s.DataType.DataType()), val)
}
