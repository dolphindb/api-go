package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"

	"github.com/stretchr/testify/assert"
)

var dataFormBytes = map[DataFormByte][]byte{
	DfScalar:     {18, 0, 115, 99, 97, 108, 97, 114, 0},
	DfTable:      {0, 6, 1, 0, 0, 0, 1, 0, 0, 0, 116, 97, 98, 108, 101, 0, 99, 111, 108, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 99, 111, 108, 49, 0, 99, 111, 108, 49, 0, 99, 111, 108, 49, 0},
	DfVector:     {68, 1, 3, 0, 0, 0, 1, 0, 0, 0, 3, 0, 1, 0, 3, 3, 3, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0},
	DfPair:       {18, 2, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0},
	DfMatrix:     {18, 3, 3, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0, 18, 3, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0},
	DfSet:        {18, 4, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0},
	DfDictionary: {18, 5, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0},
	DfChart:      {25, 7, 18, 1, 5, 0, 0, 0, 1, 0, 0, 0, 116, 105, 116, 108, 101, 0, 99, 104, 97, 114, 116, 84, 121, 112, 101, 0, 115, 116, 97, 99, 107, 105, 110, 103, 0, 100, 97, 116, 97, 0, 101, 120, 116, 114, 97, 115, 0, 25, 1, 5, 0, 0, 0, 1, 0, 0, 0, 18, 1, 1, 0, 0, 0, 1, 0, 0, 0, 99, 104, 97, 114, 116, 0, 4, 0, 4, 0, 0, 0, 1, 0, 0, 18, 3, 0, 18, 3, 3, 0, 0, 0, 1, 0, 0, 0, 109, 49, 0, 109, 50, 0, 109, 51, 0, 18, 5, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0},
}

func TestDataForm(t *testing.T) {
	by := bytes.NewBuffer([]byte{})
	r := protocol.NewReader(by)
	bo := protocol.LittleEndian

	by.Write(dataFormBytes[DfScalar])
	df, err := ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfScalar)
	assert.Equal(t, df.String(), "string(scalar)")

	by.Write(dataFormBytes[DfTable])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfTable)
	assert.Equal(t, df.String(), "table[1r][1c]([\n\t  string[3]('col', [col1, col1, col1])\n\t])")

	by.Write(dataFormBytes[DfVector])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfVector)
	assert.Equal(t, df.String(), "vector<intArray>([[1, 2, 3], [4, 5, 6], [7, 8, 9]])")

	by.Write(dataFormBytes[DfPair])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfPair)
	assert.Equal(t, df.String(), "pair<string>([key1, key2, key3])")

	by.Write(dataFormBytes[DfMatrix])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfMatrix)
	assert.Equal(t, df.String(), "matrix<string>[3r][1c]({\n  rows: [value1, value2, value3],\n  cols: [value1, value2, value3],\n  data: stringArray(3) [\n    key1,\n    key2,\n    key3,\n  ]\n})")

	by.Write(dataFormBytes[DfSet])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfSet)
	assert.Equal(t, df.String(), "set<string>[3]([key1, key2, key3])")

	by.Write(dataFormBytes[DfDictionary])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfDictionary)
	assert.Equal(t, df.String(), "dict<string, string>([\n  string[3]([key1, key2, key3]),\n  string[3]([value1, value2, value3]),\n])")

	by.Write(dataFormBytes[DfChart])
	df, err = ParseDataForm(r, bo)
	assert.Nil(t, err)
	assert.Equal(t, df.GetDataForm(), DfChart)
	assert.Equal(t, df.String(), "Chart({\n  title: [chart]\n  chartType: CT_LINE\n  stacking: false\n  data: matrix<string>[3r][1c]({\n  rows: null,\n  cols: null,\n  data: stringArray(3) [\n    m1,\n    m2,\n    m3,\n  ]\n})\n  extras: dict<string, string>([\n  string[3]([key1, key2, key3]),\n  string[3]([value1, value2, value3]),\n])\n})")
}
