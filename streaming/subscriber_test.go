package streaming

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	later := isLater("2.00.10", "2.00.9")
	assert.True(t, later)

	later = isLater("2.00.8", "2.00.9")
	assert.False(t, later)
}
