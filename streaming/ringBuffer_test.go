package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingBufferEmptyOperationsDoNotPanic(t *testing.T) {
	rb := NewRingBuffer(0)

	assert.Equal(t, 2, rb.Capacity())
	assert.Nil(t, rb.Peek())
	assert.Nil(t, rb.Pop())
}
