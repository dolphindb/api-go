package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetByteOrder(t *testing.T) {
	bo := GetByteOrder('0')
	assert.Equal(t, bo, BigEndian)

	bo = GetByteOrder('1')
	assert.Equal(t, bo, LittleEndian)
}
