package model

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
)

var originalTime = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC)

func renderDuration(val interface{}) ([2]uint32, error) {
	b, ok := val.(string)
	if !ok {
		return [2]uint32{}, errors.New("the type of in must be string when datatype is DtDuration")
	}

	return renderDurationFromString(b)
}

func renderDouble2(val interface{}) ([2]float64, error) {
	b, ok := val.([2]float64)
	if !ok {
		return [2]float64{}, errors.New("the type of in must be [2]float64 when datatype is DtComplex or DtPoint")
	}

	return b, nil
}

func renderBool(val interface{}) (uint8, error) {
	b, ok := val.(byte)
	if !ok {
		return 0, errors.New("the type of in must be byte when datatype is DtBool")
	}

	return renderBoolFromByte(b), nil
}

func renderBlob(val interface{}) ([]byte, error) {
	byt, ok := val.([]byte)
	if !ok {
		return nil, errors.New("the type of in must be []byte when datatype is DtBlob")
	}

	return byt, nil
}

func renderByte(val interface{}) (uint8, error) {
	b, ok := val.(byte)
	if !ok {
		return 0, errors.New("the type of in must be byte when datatype is DtChar or DtCompress")
	}

	return b, nil
}

func renderDate(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtDate")
	}

	return renderDateFromTime(ti), nil
}

func renderDateHour(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtDateHour")
	}

	return renderDateHourFromTime(ti), nil
}

func renderDateTime(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtDatetime")
	}

	return renderDateTimeFromTime(ti), nil
}

func renderDouble(val interface{}) (float64, error) {
	f, ok := val.(float64)
	if !ok {
		return 0, errors.New("the type of in must be float64 when datatype is DtDouble")
	}

	return f, nil
}

func renderFloat(val interface{}) (float32, error) {
	f, ok := val.(float32)
	if !ok {
		return 0, errors.New("the type of in must be float32 when datatype is DtFloat")
	}

	return f, nil
}

func renderInt(val interface{}) (int32, error) {
	i, ok := val.(int32)
	if !ok {
		return 0, errors.New("the type of in must be int32 when datatype is DtInt")
	}

	return i, nil
}

func renderInt128(val interface{}) ([2]uint64, error) {
	str, ok := val.(string)
	if !ok {
		return [2]uint64{}, errors.New("the type of in must be string when datatype is DtInt128")
	}

	return renderInt128FromString(str), nil
}

func renderIP(val interface{}, bo protocol.ByteOrder) ([2]uint64, error) {
	str, ok := val.(string)
	if !ok {
		return [2]uint64{}, errors.New("the type of in must be string when datatype is DtIP")
	}

	return renderIPFromString(str, bo), nil
}

func renderLong(val interface{}) (int64, error) {
	i, ok := val.(int64)
	if !ok {
		return 0, errors.New("the type of in must be int64 when datatype is DtLong")
	}

	return i, nil
}

func renderMinute(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtMinute")
	}

	return renderMinuteFromTime(ti), nil
}

func renderMonth(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtMonth")
	}

	return renderMonthFromTime(ti), nil
}

func renderNanoTime(val interface{}) (int64, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtNanoTime")
	}

	return renderNanoTimeFromTime(ti), nil
}

func renderNanoTimestamp(val interface{}) (int64, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtNanoTimestamp")
	}

	return renderNanoTimestampFromTime(ti), nil
}

func renderSecond(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtSecond")
	}

	return renderSecondFromTime(ti), nil
}

func renderShort(val interface{}) (int16, error) {
	i, ok := val.(int16)
	if !ok {
		return 0, errors.New("the type of in must be int16 when datatype is DtShort")
	}

	return i, nil
}

func renderTime(val interface{}) (int32, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtTime")
	}

	return renderTimeFromTime(ti), nil
}

func renderTimestamp(val interface{}) (int64, error) {
	ti, ok := val.(time.Time)
	if !ok {
		return 0, errors.New("the type of in must be time.Time when datatype is DtTimestamp")
	}

	return renderTimestampFromTime(ti), nil
}

func renderUUID(val interface{}) ([2]uint64, error) {
	str, ok := val.(string)
	if !ok {
		return [2]uint64{}, errors.New("the type of in must be string when datatype is DtUuid")
	}

	return renderUUIDFromString(str), nil
}

func renderAny(val interface{}) (DataForm, error) {
	dataForm, ok := val.(DataForm)
	if !ok {
		return nil, errors.New("the type of in must be DataForm when datatype is DtAny")
	}

	return dataForm, nil
}

func renderString(val interface{}) (string, error) {
	str, ok := val.(string)
	if !ok {
		return "", errors.New("the type of in must be string when datatype is DtString, DtCode, DtFunction, DtHandle or DtSymbol")
	}

	return str, nil
}

func renderDurationFromString(val string) ([2]uint32, error) {
	if val == "" {
		return emptyDuration, nil
	}
	data := val[:len(val)-1]
	i, err := strconv.Atoi(data)
	if err != nil {
		return [2]uint32{}, err
	}

	return [2]uint32{uint32(i), durationUnitReverse[val[len(val)-1:]]}, nil
}

func renderBoolFromByte(val byte) uint8 {
	if val == 0 || val == MinInt8 {
		return val
	}

	return 1
}

func renderDateFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	d := time.Date(ti.Year(), ti.Month(), ti.Day(), 0, 0, 0, 0, time.UTC)
	return int32(d.Unix() / 86400)
}

func renderDateHourFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	d := time.Date(ti.Year(), ti.Month(), ti.Day(), ti.Hour(), 0, 0, 0, time.UTC)
	return int32(d.Unix() / 3600)
}

func renderDateTimeFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	return int32(ti.Unix())
}

func renderInt128FromString(str string) [2]uint64 {
	if str == "" {
		return emptyInt64List
	}
	length := len(str)
	return [2]uint64{
		stringToUint64(str[length/2:]),
		stringToUint64(str[:length/2]),
	}
}

func renderIPFromString(str string, bo protocol.ByteOrder) [2]uint64 {
	if str == "" {
		return emptyInt64List
	}
	if strings.Contains(str, ":") {
		val := strings.Split(str, ":")
		return [2]uint64{
			stringToUint64(strings.Join(val[4:], "")),
			stringToUint64(strings.Join(val[:4], "")),
		}
	}

	buf := make([]uint8, 8)
	val := strings.Split(str, ".")
	for k, v := range val {
		i, err := strconv.Atoi(v)
		if err != nil {
			return [2]uint64{}
		}

		buf[4-k-1] = uint8(i)
	}

	return [2]uint64{
		bo.Uint64(protocol.ByteSliceFromUint8Slice(buf)),
		0,
	}
}

func renderMinuteFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	d := (ti.Unix() - time.Date(ti.Year(), ti.Month(), ti.Day(), 0, 0, 0, 0, time.UTC).Unix()) / 60
	return int32(d)
}

func renderMonthFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	return int32(ti.Year()*12) + int32(ti.Month()) - 1
}

func renderNanoTimeFromTime(ti time.Time) int64 {
	if ti == NullTime {
		return NullLong
	}
	ti = ti.UTC()
	return ti.Sub(time.Date(ti.Year(), ti.Month(), ti.Day(), 0, 0, 0, 0, time.UTC)).Nanoseconds()
}

func renderNanoTimestampFromTime(ti time.Time) int64 {
	if ti == NullTime {
		return NullLong
	}
	ti = ti.UTC()
	return ti.Sub(originalTime).Nanoseconds()
}

func renderSecondFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	d := ti.Unix() - time.Date(ti.Year(), ti.Month(), ti.Day(), 0, 0, 0, 0, time.UTC).Unix()
	return int32(d)
}

func renderTimeFromTime(ti time.Time) int32 {
	if ti == NullTime {
		return NullInt
	}
	ti = ti.UTC()
	d := ti.Sub(time.Date(ti.Year(), ti.Month(), ti.Day(), 0, 0, 0, 0, time.UTC)).Milliseconds()
	return int32(d)
}

func renderTimestampFromTime(ti time.Time) int64 {
	if ti == NullTime {
		return NullLong
	}
	ti = ti.UTC()
	if ti.Year() < 1970 {
		ms := ti.Sub(time.Date(ti.Year(), 1, 1, 0, 0, 0, 0, time.UTC)).Milliseconds()
		s := originalTime.Sub(time.Date(ti.Year(), 1, 1, 0, 0, 0, 0, time.UTC)).Milliseconds()
		return ms - s
	}

	return ti.Sub(originalTime).Milliseconds()
}

func renderUUIDFromString(str string) [2]uint64 {
	if str == "" || str == NullUUID {
		return emptyInt64List
	}
	val := strings.Split(str, "-")
	return [2]uint64{
		stringToUint64(strings.Join(val[3:], "")),
		stringToUint64(strings.Join(val[:3], "")),
	}
}
