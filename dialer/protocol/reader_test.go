package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader(t *testing.T) {
	by := bytes.NewBuffer([]byte("\ntest reader"))

	r := NewReader(by)
	b, err := r.ReadByte()
	assert.Nil(t, err)
	assert.Equal(t, b, NewLine)

	bs, err := r.ReadBytes(EmptySpace)
	assert.Nil(t, err)
	assert.Equal(t, string(bs), "test")

	bs, err = r.ReadCertainBytes(6)
	assert.Nil(t, err)
	assert.Equal(t, string(bs), "reader")

	_, err = r.ReadBytes(EmptySpace)
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "EOF")
}
