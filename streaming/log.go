package streaming

import (
	"fmt"

	"github.com/dolphindb/api-go/v3/logging"
)

func streamingLogInfof(format string, args ...any) {
	logging.Info("streaming", fmt.Sprintf(format, args...))
}

func streamingLogDebugf(format string, args ...any) {
	logging.Debug("streaming", fmt.Sprintf(format, args...))
}

func streamingLogWarnf(format string, args ...any) {
	logging.Warn("streaming", fmt.Sprintf(format, args...))
}

func streamingLogErrorf(format string, args ...any) {
	logging.Error("streaming", fmt.Sprintf(format, args...))
}
