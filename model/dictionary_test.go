package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

const dictExpect = "dict<string, string>([\n  string[3]([key1, key2, key3]),\n  string[3]([value1, value2, value3]),\n])"

func TestDictionary(t *testing.T) {
	keys, err := NewDataTypeListFromRawData(DtString, []string{"key1", "key2", "key3"})
	assert.Nil(t, err)

	values, err := NewDataTypeListFromRawData(DtString, []string{"value1", "value2", "value3"})
	assert.Nil(t, err)

	dict := NewDictionary(NewVector(keys), NewVector(values))
	assert.Equal(t, dict.GetDataForm(), DfDictionary)
	assert.Equal(t, dict.GetDataType(), DtString)
	assert.Equal(t, dict.GetDataTypeString(), "string")
	assert.Equal(t, dict.Rows(), 3)
	assert.Equal(t, dict.KeyStrings(), []string{"key1", "key2", "key3"})

	v, err := dict.Get("key1")
	assert.Nil(t, err)

	s := v.String()
	assert.Equal(t, s, "value1")

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = dict.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x12\x05\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00key1\x00key2\x00key3\x00\x12\x01\x03\x00\x00\x00\x01\x00\x00\x00value1\x00value2\x00value3\x00")
	assert.Equal(t, dict.String(), dictExpect)

	k, err := NewDataType(DtString, "key4")
	assert.Nil(t, err)

	v, err = NewDataType(DtString, "value4")
	assert.Nil(t, err)

	dict.Set(k, v)
	v, err = dict.Get("key4")
	assert.Nil(t, err)
	assert.Equal(t, v.String(), "value4")

	dict.Set(k, values.Get(2))
	v, err = dict.Get("key4")
	assert.Nil(t, err)
	assert.Equal(t, v.String(), "value3")
}
