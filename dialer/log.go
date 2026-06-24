package dialer

import (
	"fmt"

	"github.com/dolphindb/api-go/v3/logging"
)

func dialerLogDebugf(format string, args ...any) {
	logging.Debug("dialer", fmt.Sprintf(format, args...))
}

func dialerLogInfof(format string, args ...any) {
	logging.Info("dialer", fmt.Sprintf(format, args...))
}

func dialerLogWarnf(format string, args ...any) {
	logging.Warn("dialer", fmt.Sprintf(format, args...))
}

func dialerLogErrorf(format string, args ...any) {
	logging.Error("dialer", fmt.Sprintf(format, args...))
}

func (c *conn) dialerLogDebugf(format string, args ...any) {
	c.dialerLog("debug", fmt.Sprintf(format, args...))
}

func (c *conn) dialerLogInfof(format string, args ...any) {
	c.dialerLog("info", fmt.Sprintf(format, args...))
}

func (c *conn) dialerLogWarnf(format string, args ...any) {
	c.dialerLog("warn", fmt.Sprintf(format, args...))
}

func (c *conn) dialerLogErrorf(format string, args ...any) {
	c.dialerLog("error", fmt.Sprintf(format, args...))
}

func (c *conn) dialerLog(level string, msg string) {
	if c == nil || c.logName == "" {
		switch level {
		case "debug":
			logging.Debug("dialer", msg)
		case "warn":
			logging.Warn("dialer", msg)
		case "error":
			logging.Error("dialer", msg)
		default:
			logging.Info("dialer", msg)
		}
		return
	}

	switch level {
	case "debug":
		logging.Debug("dialer", msg, "conn", c.logName)
	case "warn":
		logging.Warn("dialer", msg, "conn", c.logName)
	case "error":
		logging.Error("dialer", msg, "conn", c.logName)
	default:
		logging.Info("dialer", msg, "conn", c.logName)
	}
}
