package model

import (
	"fmt"
	"math"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
)

func parseDate(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}

	return originalTime.Add(time.Duration(res*24) * time.Hour)
}

func parseMonth(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	year := res / 12
	month := res%12 + 1

	return time.Date(int(year), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
}

func parseTime(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Millisecond)
}

func parseMinute(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res*60) * time.Second)
}

func parseSecond(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Second)
}

func parseDateTime(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Second)
}

func parseDateMinute(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Minute)
}

func parseDateHour(raw interface{}) time.Time {
	res := raw.(int32)
	if res == math.MinInt32 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Hour)
}

func parseTimeStamp(raw interface{}) time.Time {
	res := raw.(int64)
	if res == math.MinInt64 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Millisecond)
}

func parseNanoTime(raw interface{}) time.Time {
	res := raw.(int64)
	if res == math.MinInt64 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Nanosecond)
}

func parseNanoTimeStamp(raw interface{}) time.Time {
	res := raw.(int64)
	if res == math.MinInt64 {
		return time.Time{}
	}
	return originalTime.Add(time.Duration(res) * time.Nanosecond)
}

func parseDuration(raw interface{}) string {
	du := raw.([2]uint32)
	unit := durationUnit[du[1]]
	if du[0] == MinInt32 {
		return ""
	}
	return fmt.Sprintf("%d%s", du[0], unit)
}

func parseComplex(raw interface{}) string {
	fp := raw.([2]float64)
	if fp[0] == -math.MaxFloat64 || fp[1] == -math.MaxFloat64 {
		return ""
	}
	return fmt.Sprintf("%.5f+%.5fi", fp[0], fp[1])
}

func parsePoint(raw interface{}) string {
	fp := raw.([2]float64)
	if fp[0] == -math.MaxFloat64 || fp[1] == -math.MaxFloat64 {
		return emptyPoint
	}
	return fmt.Sprintf("(%.5f, %.5f)", fp[0], fp[1])
}

func parseIP(raw interface{}, bo protocol.ByteOrder) string {
	p := raw.([2]uint64)
	if p[0] == 0 && p[1] == 0 {
		return "0.0.0.0"
	}

	low := make([]byte, 8)
	bo.PutUint64(low, p[0])
	if p[1] == 0 {
		return fmt.Sprintf("%d.%d.%d.%d", low[3], low[2], low[1], low[0])
	}

	high := make([]byte, 8)
	bo.PutUint64(high, p[1])
	return fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x", bo.Uint16(high[6:8]), bo.Uint16(high[4:6]), bo.Uint16(high[2:4]),
		bo.Uint16(high[0:2]), bo.Uint16(low[6:8]), bo.Uint16(low[4:6]), bo.Uint16(low[2:4]), bo.Uint16(low[0:2]))
}

func parseUUID(raw interface{}, bo protocol.ByteOrder) string {
	p := raw.([2]uint64)
	if p[0] == 0 || p[1] == 0 {
		return "00000000-0000-0000-0000-000000000000"
	}

	high, low := make([]byte, 8), make([]byte, 8)
	bo.PutUint64(high, p[1])
	bo.PutUint64(low, p[0])

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", bo.Uint32(high[4:]), bo.Uint16(high[2:4]),
		bo.Uint16(high[0:2]), bo.Uint16(low[6:8]), bo.Uint64(append(low[0:6], 0, 0)))
}

func parseInt128(raw interface{}) string {
	p := raw.([2]uint64)
	if p[0] == 0 && p[1] == 0 {
		return "00000000000000000000000000000000"
	}

	return fmt.Sprintf("%016x%016x", p[1], p[0])
}
