package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"

	"github.com/stretchr/testify/assert"
)

const chartExpect = "Chart({\n  title: [chart xaxis yaxis]\n  chartType: CT_LINE\n  stacking: false\n  data: matrix<string>[3r][1c]({\n  rows: null,\n  cols: null,\n  data: stringArray(3) [\n    m1,\n    m2,\n    m3,\n  ]\n})\n  extras: dict<string, string>([\n  string[3]([key1, key2, key3]),\n  string[3]([value1, value2, value3]),\n])\n})"

func TestChart(t *testing.T) {
	dtl, err := NewDataTypeListFromRawData(DtString, []string{"chart", "xaxis", "yaxis"})
	assert.Nil(t, err)
	assert.Equal(t, dtl.DataType(), DtString)

	ti := NewVector(dtl)

	dt, err := NewDataType(DtInt, int32(4))
	assert.Nil(t, err)

	ct := NewScalar(dt)

	dt, err = NewDataType(DtBool, byte(0))
	assert.Nil(t, err)

	st := NewScalar(dt)

	d, err := NewDataTypeListFromRawData(DtString, []string{"m1", "m2", "m3"})
	assert.Nil(t, err)

	data := NewMatrix(NewVector(d), nil, nil)

	keys, err := NewDataTypeListFromRawData(DtString, []string{"key1", "key2", "key3"})
	assert.Nil(t, err)

	values, err := NewDataTypeListFromRawData(DtString, []string{"value1", "value2", "value3"})
	assert.Nil(t, err)

	extras := NewDictionary(NewVector(keys), NewVector(values))

	ch := NewChart(map[string]DataForm{
		"title":     ti,
		"chartType": ct,
		"stacking":  st,
		"data":      data,
		"extras":    extras,
	})
	assert.Equal(t, ch.GetDataForm(), DfChart)
	assert.Equal(t, ch.GetDataType(), DtAny)
	assert.Equal(t, ch.GetDataTypeString(), "any")
	assert.Equal(t, ch.Rows(), 5)
	assert.Equal(t, ch.GetTitle(), "chart")
	assert.Equal(t, ch.GetXAxisName(), "xaxis")
	assert.Equal(t, ch.GetYAxisName(), "yaxis")
	assert.Equal(t, ch.GetChartType(), "CT_LINE")

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = ch.Render(w, protocol.LittleEndian)
	w.Flush()

	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x19\a\x12\x01\x05\x00\x00\x00\x01\x00\x00\x00title\x00chartType\x00stacking\x00data\x00extras\x00\x19\x01\x05\x00\x00\x00\x01\x00\x00\x00\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00chart\x00xaxis\x00yaxis\x00\x04\x00\x04\x00\x00\x00\x01\x00\x00\x12\x03\x00\x12\x03\x03\x00\x00\x00\x01\x00\x00\x00m1\x00m2\x00m3\x00\x12\x05\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00key1\x00key2\x00key3\x00\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00value1\x00value2\x00value3\x00")
	assert.Equal(t, ch.String(), chartExpect)
}
