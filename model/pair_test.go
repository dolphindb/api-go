package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

const pairExpect = "pair<string>([key1, key2])"

func TestPair(t *testing.T) {
	data, err := NewDataTypeListWithRaw(DtString, []string{"key1", "key2"})
	assert.Nil(t, err)

	pair := NewPair(NewVector(data))
	assert.Equal(t, pair.GetDataForm(), DfPair)
	assert.Equal(t, pair.GetDataType(), DtString)
	assert.Equal(t, pair.GetDataTypeString(), "string")
	assert.Equal(t, pair.Rows(), 2)

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = pair.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x12\x02\x02\x00\x00\x00\x01\x00\x00\x00key1\x00key2\x00")
	assert.Equal(t, pair.String(), pairExpect)
}
