package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

const setExpect = "set<string>[3]([key1, key2, key3])"

func TestSet(t *testing.T) {
	data, err := NewDataTypeListWithRaw(DtString, []string{"key1", "key2", "key3"})
	assert.Nil(t, err)

	set := NewSet(NewVector(data))
	assert.Equal(t, set.GetDataForm(), DfSet)
	assert.Equal(t, set.GetDataType(), DtString)
	assert.Equal(t, set.GetDataTypeString(), "string")
	assert.Equal(t, set.Rows(), 3)

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = set.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x12\x04\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00key1\x00key2\x00key3\x00")
	assert.Equal(t, set.String(), setExpect)
}
