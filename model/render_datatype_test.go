package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderDataType(t *testing.T) {
	du, err := renderDuration("10H")
	assert.Nil(t, err)
	assert.Equal(t, du[0], uint32(10))
	assert.Equal(t, du[1], uint32(5))

	fp, err := renderDouble2([2]float64{1, 1})
	assert.Nil(t, err)
	assert.Equal(t, fp[0], float64(1))
	assert.Equal(t, fp[1], float64(1))
}
