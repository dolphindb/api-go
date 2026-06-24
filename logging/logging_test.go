package logging

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggerDefaultsToSlogDefault(t *testing.T) {
	assert.Same(t, slog.Default(), Logger())
}

func TestSetLoggerNilRestoresSlogDefault(t *testing.T) {
	originalLogger := Logger()
	defer SetLogger(originalLogger)

	var logBuf bytes.Buffer
	SetLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))

	assert.NotSame(t, slog.Default(), Logger())

	SetLogger(nil)

	assert.Same(t, slog.Default(), Logger())
}
