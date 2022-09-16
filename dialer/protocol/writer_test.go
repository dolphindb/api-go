package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	by := bytes.NewBuffer(nil)
	w := NewWriter(by)
	err := w.WriteByte(byte(0))
	assert.Nil(t, err)

	err = w.Write([]byte{1, 2})
	assert.Nil(t, err)

	err = w.WriteString("test")
	assert.Nil(t, err)

	err = w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x1, 0x2, 0x74, 0x65, 0x73, 0x74})
}
