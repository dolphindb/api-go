package api

import (
	"fmt"

	"github.com/dolphindb/api-go/v3/logging"
)

func apiLogInfof(format string, args ...any) {
	logging.Info("api", fmt.Sprintf(format, args...))
}

func apiLogDebugf(format string, args ...any) {
	logging.Debug("api", fmt.Sprintf(format, args...))
}

func apiLogWarnf(format string, args ...any) {
	logging.Warn("api", fmt.Sprintf(format, args...))
}

func apiLogErrorf(format string, args ...any) {
	logging.Error("api", fmt.Sprintf(format, args...))
}

func (d *DBConnectionPool) apiLogInfof(format string, args ...any) {
	d.apiLog("info", fmt.Sprintf(format, args...))
}

func (d *DBConnectionPool) apiLogDebugf(format string, args ...any) {
	d.apiLog("debug", fmt.Sprintf(format, args...))
}

func (d *DBConnectionPool) apiLogWarnf(format string, args ...any) {
	d.apiLog("warn", fmt.Sprintf(format, args...))
}

func (d *DBConnectionPool) apiLogErrorf(format string, args ...any) {
	d.apiLog("error", fmt.Sprintf(format, args...))
}

func (d *DBConnectionPool) apiLog(level string, msg string) {
	if d == nil || d.logName == "" {
		switch level {
		case "debug":
			logging.Debug("api", msg)
		case "warn":
			logging.Warn("api", msg)
		case "error":
			logging.Error("api", msg)
		default:
			logging.Info("api", msg)
		}
		return
	}

	switch level {
	case "debug":
		logging.Debug("api", msg, "pool", d.logName)
	case "warn":
		logging.Warn("api", msg, "pool", d.logName)
	case "error":
		logging.Error("api", msg, "pool", d.logName)
	default:
		logging.Info("api", msg, "pool", d.logName)
	}
}
