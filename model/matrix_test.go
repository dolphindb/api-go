package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

const matrixExpect = "matrix<string>[3r][1c]({\n  rows: [value1, value2, value3],\n  cols: [value1, value2, value3],\n  data: stringArray(3) [\n    key1,\n    key2,\n    key3,\n  ]\n})"

func TestMatrix(t *testing.T) {
	data, err := NewDataTypeListWithRaw(DtString, []string{"key1", "key2", "key3"})
	assert.Nil(t, err)

	rl, err := NewDataTypeListWithRaw(DtString, []string{"value1", "value2", "value3"})
	assert.Nil(t, err)

	cl, err := NewDataTypeListWithRaw(DtString, []string{"value1", "value2", "value3"})
	assert.Nil(t, err)

	mtx := NewMatrix(NewVector(data), NewVector(rl), NewVector(cl))
	assert.Equal(t, mtx.GetDataForm(), DfMatrix)
	assert.Equal(t, mtx.GetDataType(), DtString)
	assert.Equal(t, mtx.GetDataTypeString(), "string")
	assert.Equal(t, mtx.Rows(), 3)

	dt := mtx.Get(2, 0)
	assert.Equal(t, dt.String(), "key3")
	assert.False(t, mtx.IsNull(2, 0))

	mtx.SetNull(2, 0)
	assert.True(t, mtx.IsNull(2, 0))
	assert.Equal(t, mtx.Get(2, 0).String(), "")

	dt, err = NewDataType(DtString, "key3")
	assert.Nil(t, err)

	err = mtx.Set(2, 0, dt)
	assert.Nil(t, err)
	assert.False(t, mtx.IsNull(2, 0))

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = mtx.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x12\x03\x03\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00value1\x00value2\x00value3\x00\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00value1\x00value2\x00value3\x00\x12\x03\x03\x00\x00\x00\x01\x00\x00\x00key1\x00key2\x00key3\x00")
	assert.Equal(t, mtx.String(), matrixExpect)
}
