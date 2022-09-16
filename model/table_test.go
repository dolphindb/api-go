package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

const tableExpect = "table[3r][1c]([\n\t  string[3]('col', [col1, col2, col3])\n\t])"

func TestTable(t *testing.T) {
	col, err := NewDataTypeListWithRaw(DtString, []string{"col1", "col2", "col3"})
	assert.Nil(t, err)

	tb := NewTable([]string{"col"}, []*Vector{NewVector(col)})
	assert.Equal(t, tb.GetDataForm(), DfTable)
	assert.Equal(t, tb.Rows(), 3)
	assert.Equal(t, tb.GetDataType(), DtVoid)
	assert.Equal(t, tb.GetDataTypeString(), "void")

	colNames := tb.GetColumnNames()
	assert.Equal(t, colNames, []string{"col"})

	colV := tb.GetColumnByName(colNames[0])
	assert.Equal(t, colV.String(), "vector<string>([col1, col2, col3])")

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = tb.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x00\x06\x03\x00\x00\x00\x01\x00\x00\x00\x00col\x00\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00col1\x00col2\x00col3\x00")
	assert.Equal(t, tb.String(), tableExpect)

	tb = tb.GetSubtable([]int{0, 2})
	colV = tb.GetColumnByIndex(0)
	assert.Equal(t, colV.String(), "vector<string>([col1, col3])")
	assert.Equal(t, tb.Columns(), 1)
}
