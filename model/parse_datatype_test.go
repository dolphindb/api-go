package model

import (
	"math"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"

	"github.com/stretchr/testify/assert"
)

func TestParseDataType(t *testing.T) {
	bo := protocol.LittleEndian

	res := parseDate(int32(4))
	assert.Equal(t, res.Format("2006.01.02"), "1970.01.05")

	res = parseMonth(int32(24244))
	assert.Equal(t, res.Format("2006.01M"), "2020.05M")

	res = parseTimeStamp(int64(360000000))
	assert.Equal(t, res.Format("2006.01.02T15:04:05.000"), "1970.01.05T04:00:00.000")

	res = parseMinute(int32(6000))
	assert.Equal(t, res.Format("15:04M"), "04:00M")

	res = parseDateHour(int32(100))
	assert.Equal(t, res.Format("2006.01.02T15"), "1970.01.05T04")

	res = parseDateTime(int32(360000))
	assert.Equal(t, res.Format("2006.01.02T15:04:05"), "1970.01.05T04:00:00")

	res = parseSecond(int32(360000))
	assert.Equal(t, res.Format("15:04:05"), "04:00:00")

	res = parseMinute(int32(6000))
	assert.Equal(t, res.Format("15:04M"), "04:00M")

	res = parseTime(int32(360000000))
	assert.Equal(t, res.Format("15:04:05.000"), "04:00:00.000")

	high := int64(-2204767551958936073)
	intP := [2]uint64{
		7149476803327945778,
		uint64(high),
	}
	ti := parseInt128(intP)
	assert.Equal(t, ti, "e1671797c52e15f763380b45e841ec32")

	low := int64(-4675754494756414117)
	high = int64(-1878940850640566832)
	intP = [2]uint64{
		uint64(low),
		uint64(high),
	}
	ti = parseUUID(intP, bo)
	assert.Equal(t, ti, "e5eca940-5b99-45d0-bf1c-620f6b1b9d5b")

	intP = [2]uint64{
		8526542638814027207,
		3777231640985064004,
	}
	ti = parseIP(intP, bo)
	assert.Equal(t, ti, "346b:6c2a:3347:d244:7654:5d5a:bcbb:5dc7")

	fp := [2]float64{
		math.Float64frombits(4607182418800017408),
		math.Float64frombits(4607182418800017408),
	}
	ti = parseComplex(fp)
	assert.Equal(t, ti, "1.00000+1.00000i")

	du := [2]uint32{
		10,
		5,
	}
	ti = parseDuration(du)
	assert.Equal(t, ti, "10H")

	res = parseNanoTimeStamp(int64(360000000000000))
	assert.Equal(t, res.Format("2006.01.02T15:04:05.000000000"), "1970.01.05T04:00:00.000000000")

	res = parseNanoTime(int64(360000000000000))
	assert.Equal(t, res.Format("15:04:05.000000000"), "04:00:00.000000000")
}
