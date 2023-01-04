package model

import (
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Pair is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Pair.html for details.
type Pair struct {
	category *Category

	Vector *Vector
}

// NewPair returns an object of pair with specified vector v.
// You can instantiate it by NewVector.
func NewPair(v *Vector) *Pair {
	if v.Rows() != 2 {
		fmt.Println("[ERROR] The Vector must be of length 2 when initializing a Pair.")
	}

	return &Pair{
		category: &Category{
			DataForm: DfPair,
			DataType: v.GetDataType(),
		},
		Vector: v,
	}
}

// Rows returns the row num of the DataForm.
func (p *Pair) Rows() int {
	return int(p.Vector.RowCount)
}

// GetDataForm returns the byte type of the DataForm.
func (p *Pair) GetDataForm() DataFormByte {
	return DfPair
}

// GetDataType returns the byte type of the DataType.
func (p *Pair) GetDataType() DataTypeByte {
	return p.category.DataType
}

// GetDataTypeString returns the string format of the DataType.
func (p *Pair) GetDataTypeString() string {
	return GetDataTypeString(p.category.DataType)
}

// Render serializes the DataForm with bo and input it into w.
func (p *Pair) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	err := p.category.render(w)
	if err != nil {
		return err
	}

	buf := make([]byte, 8)

	bo.PutUint32(buf[:4], 2)
	bo.PutUint32(buf[4:], 1)
	err = w.Write(buf)
	if err != nil {
		return err
	}

	return p.Vector.renderData(w, bo)
}

func (p *Pair) String() string {
	if p.Vector == nil {
		return ""
	}

	val := p.Vector.formatString()
	return fmt.Sprintf("pair<%s>([%s])", GetDataTypeString(p.Vector.GetDataType()), strings.Join(val, ", "))
}
