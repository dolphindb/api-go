package model

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
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

// NewTableFromRawData returns an object of Table with colNames, colTypes and colValues.
// The parameter colTypes determines the data types of colValues.
// Refer to README.md for more details.
func NewTableFromRawData(colNames []string, colTypes []DataTypeByte, colValues []interface{}) (*Table, error) {
	if len(colNames) != len(colTypes) || len(colNames) != len(colValues) {
		return nil, errors.New("The length of colNames, colTypes and colValues should be equal.")
	}
	vcts := make([]*Vector, len(colNames))
	for k, v := range colTypes {
		dtl, err := NewDataTypeListFromRawData(v, colValues[k])
		if err != nil {
			return nil, err
		}

		vcts[k] = NewVector(dtl)
	}

	return NewTable(colNames, vcts), nil
}

// NewTableFromStruct returns the table object according to the val which is a struct object with special tags.
// Refer to README.md for more details.
func NewTableFromStruct(obj interface{}) (tb *Table, err error) {
	if obj == nil {
		return nil, errors.New("Input should not be nil")
	}

	defer func() {
		e := recover()
		if e != nil {
			errMsg := fmt.Sprintf("%v", e)
			err = errors.New(errMsg)
		}
	}()

	value := reflect.ValueOf(obj).Elem()
	dataType := value.Type()
	colNum := dataType.NumField()
	colNames := make([]string, colNum)
	colTypes := make([]DataTypeByte, colNum)
	colValues := make([]interface{}, colNum)
	for i := 0; i < colNum; i++ {
		field := dataType.Field(i)
		raw, containsNameTag := field.Tag.Lookup("dolphindb")
		if !containsNameTag || raw == "" {
			continue
		}

		tags := parseTags(raw)
		colNames[i] = tags["column"]
		colValues[i] = value.Field(i).Interface()
		colTypes[i] = dataTypeByteMap[tags["type"]]
		if colTypes[i] == 0 {
			return nil, fmt.Errorf("Invalid type %s", tags["type"])
		}
	}

	return NewTableFromRawData(colNames, colTypes, colValues)
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

// GetRowJSON returns the string format of the row in the table according to the ind.
// ArrayVector does not support GetRowJSON.
func (t *Table) GetRowJSON(ind int) string {
	if ind >= t.Rows() {
		return ""
	}

	buf := bytes.NewBuffer(nil)
	buf.WriteString("{")
	for k, v := range t.columnValues {
		buf.WriteString(fmt.Sprintf("\"%s\"", t.ColNames[k]))
		buf.WriteString(":")
		buf.WriteString(fmt.Sprintf("\"%s\"", v.Get(ind).String()))
		buf.WriteString(",")
	}

	if buf.Len() > 1 {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteString("}")

	return buf.String()
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

// GetDataFormString returns the string format of the DataForm.
func (t *Table) GetDataFormString() string {
	return GetDataFormString(t.category.DataForm)
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

	var symBases *symbolBaseCollection
	for _, v := range t.columnValues {
		if v.Extend != nil {
			if symBases == nil {
				symBases = &symbolBaseCollection{}
			}
			err = v.renderSymbolExtendVector(w, bo, symBases)
		} else {
			err = v.Render(w, bo)
		}
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
		colVal := t.columnValues[k]
		val := colVal.formatString()

		dt := GetDataTypeString(colVal.GetDataType())
		if len(val) == 0 {
			by.WriteString(fmt.Sprintf("  %s[%d]('%s', null)\n\t", dt, colVal.RowCount, v))
		} else {
			by.WriteString(fmt.Sprintf("  %s[%d]('%s', [%s])\n\t", dt, colVal.RowCount, v, strings.Join(val, ", ")))
		}
	}
	by.WriteString("])")

	return by.String()
}
