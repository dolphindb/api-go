package util

import (
	"log/slog"
	"os"

	"github.com/dolphindb/api-go/v3/logging"
)

// InitExampleLogger installs a simple text logger for runnable examples.
func InitExampleLogger() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	logging.SetLogger(logger)
}
