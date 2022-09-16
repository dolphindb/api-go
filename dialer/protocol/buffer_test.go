package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {
	r := bytes.NewReader([]byte{1, 0, 0, 0, 10})

	buf := NewBuffer(1, NewReader(r))
	blobs, err := buf.ReadBlobs(LittleEndian)
	assert.Nil(t, err)
	assert.Equal(t, len(blobs), 1)
	assert.Equal(t, blobs[0][0], byte(10))
}
