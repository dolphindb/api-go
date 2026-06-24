package multigoroutinetable

import (
	"fmt"

	"github.com/dolphindb/api-go/v3/logging"
)

func multiGoroutineTableLogInfof(format string, args ...any) {
	logging.Info("multigoroutinetable", fmt.Sprintf(format, args...))
}

func multiGoroutineTableLogWarnf(format string, args ...any) {
	logging.Warn("multigoroutinetable", fmt.Sprintf(format, args...))
}

func multiGoroutineTableLogErrorf(format string, args ...any) {
	logging.Error("multigoroutinetable", fmt.Sprintf(format, args...))
}
