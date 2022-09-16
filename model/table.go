package model

import (
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Table is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Table.html for more details.
type Table struct {
	category     *Category
	columnNames  DataTypeList
	columnValues []*Vector
	tableName    DataType
	rowCount     uint32
	columnCount  uint32

	ColNames []string
}

// NewTable returns an object of Table with colNames and colValues.
// You can instantiate the vector object by NewVector.
func NewTable(colNames []string, colValues []*Vector) *Table {
	if len(colNames) != len(colValues) {
		return nil
	}

	names := make([]DataType, len(colNames))

	for k, v := range colNames {
		dt, _ := NewDataType(DtString, v)
		names[k] = dt
	}

	tbName, _ := NewDataType(DtString, "")
	rowCount := 0
	if len(colValues) > 0 {
		rowCount = colValues[0].Rows()
	}

	return &Table{
		category: &Category{
			DataForm: DfTable,
			DataType: DtVoid,
		},
		ColNames:     colNames,
		columnNames:  NewDataTypeList(DtString, names),
		columnValues: colValues,
		columnCount:  uint32(len(colNames)),
		tableName:    tbName,
		rowCount:     uint32(rowCount),
	}
}

// Rows returns the row num of the DataForm.
func (t *Table) Rows() int {
	return int(t.rowCount)
}

// Columns returns the column num of the DataForm.
func (t *Table) Columns() int {
	return int(t.columnCount)
}

// GetDataForm returns the byte type of the DataForm.
func (t *Table) GetDataForm() DataFormByte {
	return DfTable
}

// GetSubtable instantiates a table with the values in indexes.
// The specified indexes should be less than the number of columns.
func (t *Table) GetSubtable(indexes []int) *Table {
	lenCol := len(t.columnValues)
	cols := make([]*Vector, lenCol)
	for i := 0; i < lenCol; i++ {
		cols[i] = t.columnValues[i].GetSubvector(indexes)
	}

	return NewTable(t.ColNames, cols)
}

// GetDataType returns the byte type of the DataType.
func (t *Table) GetDataType() DataTypeByte {
	return t.category.DataType
}

// GetDataTypeString returns the string format of the DataType.
func (t *Table) GetDataTypeString() string {
	return GetDataTypeString(t.category.DataType)
}

// GetColumnByName returns the column in table with the column name.
func (t *Table) GetColumnByName(colName string) *Vector {
	for k, v := range t.ColNames {
		if v == colName {
			return t.columnValues[k]
		}
	}

	return nil
}

// GetColumnByIndex returns the column in table with the column index.
func (t *Table) GetColumnByIndex(ind int) *Vector {
	if ind >= int(t.columnCount) {
		return nil
	}

	return t.columnValues[ind]
}

// GetColumnNames returns all column names of the table.
func (t *Table) GetColumnNames() []string {
	return t.ColNames
}

// Render serializes the DataForm with bo and input it into w.
func (t *Table) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	err := t.category.render(w)
	if err != nil {
		return err
	}

	err = t.renderLength(w, bo)
	if err != nil {
		return err
	}

	err = t.tableName.Render(w, bo)
	if err != nil {
		return err
	}

	err = t.columnNames.Render(w, bo)
	if err != nil {
		return err
	}

	for _, v := range t.columnValues {
		err = v.Render(w, bo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) renderLength(w *protocol.Writer, bo protocol.ByteOrder) error {
	buf := make([]byte, 8)
	bo.PutUint32(buf[0:4], t.rowCount)
	bo.PutUint32(buf[4:8], t.columnCount)

	return w.Write(buf)
}

// String returns the string of the DataForm.
func (t *Table) String() string {
	by := strings.Builder{}
	by.WriteString(fmt.Sprintf("table[%dr][%dc]([\n\t", t.rowCount, t.columnCount))

	for k, v := range t.ColNames {
		val := t.columnValues[k].formatString()

		dt := GetDataTypeString(t.columnValues[k].GetDataType())
		if len(val) == 0 {
			by.WriteString(fmt.Sprintf("  %s[%d]('%s', null)\n\t", dt, t.columnValues[k].RowCount, v))
		} else {
			by.WriteString(fmt.Sprintf("  %s[%d]('%s', [%s])\n\t", dt, t.columnValues[k].RowCount, v, strings.Join(val, ", ")))
		}
	}
	by.WriteString("])")

	return by.String()
}
