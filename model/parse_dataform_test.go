package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"

	"github.com/stretchr/testify/assert"
)

var rawDataFormBytes = map[DataFormByte][]byte{
	DfScalar:     {115, 99, 97, 108, 97, 114, 0},
	DfTable:      {1, 0, 0, 0, 1, 0, 0, 0, 116, 97, 98, 108, 101, 0, 99, 111, 108, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 99, 111, 108, 49, 0, 99, 111, 108, 49, 0, 99, 111, 108, 49, 0},
	DfVector:     {1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 118, 101, 99, 116, 111, 114, 0, 118, 101, 99, 116, 111, 114, 0, 0, 0, 0, 0},
	DfPair:       {3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0},
	DfMatrix:     {3, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0, 18, 3, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0},
	DfSet:        {18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0},
	DfDictionary: {18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0},
	DfChart:      {18, 1, 5, 0, 0, 0, 1, 0, 0, 0, 116, 105, 116, 108, 101, 0, 99, 104, 97, 114, 116, 84, 121, 112, 101, 0, 115, 116, 97, 99, 107, 105, 110, 103, 0, 100, 97, 116, 97, 0, 101, 120, 116, 114, 97, 115, 0, 25, 1, 5, 0, 0, 0, 1, 0, 0, 0, 18, 1, 1, 0, 0, 0, 1, 0, 0, 0, 99, 104, 97, 114, 116, 0, 4, 0, 4, 0, 0, 0, 1, 0, 0, 18, 3, 0, 18, 3, 3, 0, 0, 0, 1, 0, 0, 0, 109, 49, 0, 109, 50, 0, 109, 51, 0, 18, 5, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 107, 101, 121, 49, 0, 107, 101, 121, 50, 0, 107, 101, 121, 51, 0, 18, 1, 3, 0, 0, 0, 1, 0, 0, 0, 118, 97, 108, 117, 101, 49, 0, 118, 97, 108, 117, 101, 50, 0, 118, 97, 108, 117, 101, 51, 0},
}

func TestParseDataForm(t *testing.T) {
	by := bytes.NewBuffer([]byte{})
	r := protocol.NewReader(by)
	bo := protocol.LittleEndian

	by.Write(rawDataFormBytes[DfDictionary])
	c := newCategory(byte(DfDictionary), byte(DtString))
	dict, err := parseDictionary(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, dict.String(), "dict<string, string>([\n  string[3]([key1, key2, key3]),\n  string[3]([value1, value2, value3]),\n])")

	by.Write(rawDataFormBytes[DfMatrix])
	c = newCategory(byte(DfMatrix), byte(DtString))
	m, err := parseMatrix(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, m.String(), "matrix<string>[3r][1c]({\n  rows: [value1, value2, value3],\n  cols: [value1, value2, value3],\n  data: stringArray(3) [\n    key1,\n    key2,\n    key3,\n  ]\n})")

	by.Write(rawDataFormBytes[DfPair])
	c = newCategory(byte(DfPair), byte(DtString))
	pair, err := parsePair(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, pair.String(), "pair<string>([key1, key2, key3])")

	by.Write(rawDataFormBytes[DfSet])
	c = newCategory(byte(DfSet), byte(DtString))
	set, err := parseSet(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, set.String(), "set<string>[3]([key1, key2, key3])")

	by.Write(rawDataFormBytes[DfTable])
	c = newCategory(byte(DfTable), byte(DtVoid))
	tb, err := parseTable(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, tb.String(), "table[1r][1c]([\n\t  string[3]('col', [col1, col1, col1])\n\t])")

	by.Write(rawDataFormBytes[DfVector])
	c = newCategory(byte(DfVector), 145)
	vc, err := parseVector(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, vc.String(), "vector<symbolExtend>([vector])")

	by.Write(rawDataFormBytes[DfScalar])
	c = newCategory(byte(DfScalar), byte(DtString))
	sca, err := parseScalar(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, sca.String(), "string(scalar)")

	by.Write(rawDataFormBytes[DfChart])
	c = newCategory(byte(DfChart), byte(DtAny))
	ch, err := parseChart(r, bo, c)
	assert.Nil(t, err)
	assert.Equal(t, ch.String(), "Chart({\n  title: [chart]\n  chartType: CT_LINE\n  stacking: false\n  data: matrix<string>[3r][1c]({\n  rows: null,\n  cols: null,\n  data: stringArray(3) [\n    m1,\n    m2,\n    m3,\n  ]\n})\n  extras: dict<string, string>([\n  string[3]([key1, key2, key3]),\n  string[3]([value1, value2, value3]),\n])\n})")
}
