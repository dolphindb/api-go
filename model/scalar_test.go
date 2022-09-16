package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/stretchr/testify/assert"
)

const scalarExpect = "string(scalar)"

func TestScalar(t *testing.T) {
	data, err := NewDataType(DtString, "scalar")
	assert.Nil(t, err)

	s := NewScalar(data)
	assert.Equal(t, s.GetDataForm(), DfScalar)
	assert.Equal(t, s.GetDataType(), DtString)
	assert.Equal(t, s.GetDataTypeString(), "string")
	assert.Equal(t, s.Rows(), 1)

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = s.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x12\x00scalar\x00")
	assert.Equal(t, s.String(), scalarExpect)

	assert.False(t, s.IsNull())
	s.SetNull()
	assert.True(t, s.IsNull())
}
