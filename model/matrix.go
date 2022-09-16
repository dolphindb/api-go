package model

import (
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Matrix is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Matrix.html for details.
type Matrix struct {
	// The first bit of the first byte determines if the matrix has row labels
	// The second bit of the first byte determines if the matrix has column labels
	category *Category

	RowLabels    *Vector
	ColumnLabels *Vector
	Data         *Vector
}

// NewMatrix returns an object of matrix according to data, rowLabels and columnLabels.
// RowLabels and columnLabels are optional.
// You can instantiate the Vector object by using NewVector.
func NewMatrix(data, rowLabels, columnLabels *Vector) *Matrix {
	return &Matrix{
		category: &Category{
			DataForm: DfMatrix,
			DataType: data.GetDataType(),
		},
		Data:         data,
		RowLabels:    rowLabels,
		ColumnLabels: columnLabels,
	}
}

// GetDataForm returns the byte type of the DataForm.
func (mtx *Matrix) GetDataForm() DataFormByte {
	return DfMatrix
}

// GetDataType returns the byte type of the DataType.
func (mtx *Matrix) GetDataType() DataTypeByte {
	return mtx.category.DataType
}

// GetDataTypeString returns the string format of the DataType.
func (mtx *Matrix) GetDataTypeString() string {
	return GetDataTypeString(mtx.category.DataType)
}

// Rows returns the row num of the DataForm.
func (mtx *Matrix) Rows() int {
	return int(mtx.Data.RowCount)
}

// Get gets DataType from matrix.
func (mtx *Matrix) Get(row, col int) DataType {
	return mtx.Data.Get(mtx.getIndex(row, col))
}

// Set sets DataType for mtx according to the row and col.
// index = col*row + row.
// If index >= len(mtx.Data), return an error.
// If index < len(mtx.Data), cover the original value.
func (mtx *Matrix) Set(row, col int, d DataType) error {
	return mtx.Data.Set(mtx.getIndex(row, col), d)
}

// SetNull set the DataType in Matrix to null according to the row and col.
func (mtx *Matrix) SetNull(row, col int) {
	mtx.Data.SetNull(mtx.getIndex(row, col))
}

// IsNull checks whether the value located by row and col is null.
func (mtx *Matrix) IsNull(row, col int) bool {
	return mtx.Data.IsNull(mtx.getIndex(row, col))
}

// Render serializes the DataForm with bo and input it into w.
func (mtx *Matrix) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	err := mtx.category.render(w)
	if err != nil {
		return err
	}

	labelFlag := 0
	if mtx.RowLabels != nil {
		labelFlag++
	}

	if mtx.ColumnLabels != nil {
		labelFlag += 2
	}

	err = w.Write([]byte{byte(labelFlag)})
	if err != nil {
		return err
	}

	if mtx.RowLabels != nil {
		err = mtx.RowLabels.Render(w, bo)
		if err != nil {
			return err
		}
	}

	if mtx.ColumnLabels != nil {
		err = mtx.ColumnLabels.Render(w, bo)
		if err != nil {
			return err
		}
	}

	err = mtx.category.render(w)
	if err != nil {
		return err
	}

	err = mtx.Data.renderLength(w, bo)
	if err != nil {
		return err
	}

	return mtx.Data.Data.Render(w, bo)
}

func (mtx *Matrix) String() string {
	if mtx.Data == nil {
		return ""
	}

	by := strings.Builder{}
	by.WriteString(fmt.Sprintf("matrix<%s>[%dr][%dc]({\n", GetDataTypeString(mtx.Data.GetDataType()),
		mtx.Data.RowCount, mtx.Data.ColumnCount))

	if mtx.RowLabels != nil && mtx.RowLabels.Data != nil {
		val := mtx.RowLabels.formatString()
		by.WriteString(fmt.Sprintf("  rows: [%s],\n", strings.Join(val, ", ")))
	} else {
		by.WriteString("  rows: null,\n")
	}

	if mtx.ColumnLabels != nil && mtx.ColumnLabels.Data != nil {
		val := mtx.ColumnLabels.formatString()
		by.WriteString(fmt.Sprintf("  cols: [%s],\n", strings.Join(val, ", ")))
	} else {
		by.WriteString("  cols: null,\n")
	}

	if mtx.Data != nil && mtx.Data.Data != nil {
		val := mtx.Data.formatString()
		by.WriteString(fmt.Sprintf("  data: %sArray(%d) [\n", GetDataTypeString(mtx.Data.GetDataType()),
			mtx.Data.ColumnCount*mtx.Data.RowCount))
		for _, v := range val {
			by.WriteString(fmt.Sprintf("    %s,\n", v))
		}

		by.WriteString("  ]\n")
	} else {
		by.WriteString("  data: null,\n")
	}

	by.WriteString("})")
	return by.String()
}

func (mtx *Matrix) getIndex(row, col int) int {
	return col*mtx.Rows() + row
}
