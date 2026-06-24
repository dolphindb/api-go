package logging

import (
	"log/slog"
	"sync"
)

var (
	loggerMu      sync.RWMutex
	defaultLogger = slog.Default()
)

// Logger returns the SDK-wide logger.
func Logger() *slog.Logger {
	loggerMu.RLock()
	defer loggerMu.RUnlock()

	return defaultLogger
}

// SetLogger configures the SDK-wide logger.
// Passing nil resets logging to slog.Default().
func SetLogger(logger *slog.Logger) {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	if logger == nil {
		defaultLogger = slog.Default()
		return
	}

	defaultLogger = logger
}

// Debug logs a debug message for the given component.
func Debug(component, msg string, args ...any) {
	componentLogger(component).Debug(msg, args...)
}

// Info logs an informational message for the given component.
func Info(component, msg string, args ...any) {
	componentLogger(component).Info(msg, args...)
}

// Warn logs a warning message for the given component.
func Warn(component, msg string, args ...any) {
	componentLogger(component).Warn(msg, args...)
}

// Error logs an error message for the given component.
func Error(component, msg string, args ...any) {
	componentLogger(component).Error(msg, args...)
}

func componentLogger(component string) *slog.Logger {
	logger := Logger()
	if component == "" {
		return logger
	}

	return logger.With("component", component)
}
