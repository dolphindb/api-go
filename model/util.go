package model

import (
	"errors"
	"strconv"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
)

var dataTypeStringMap = map[DataTypeByte]string{
	DtVoid:          "void",
	DtBool:          "bool",
	DtChar:          "char",
	DtShort:         "short",
	DtInt:           "int",
	DtLong:          "long",
	DtDate:          "date",
	DtMonth:         "month",
	DtTime:          "time",
	DtMinute:        "minute",
	DtSecond:        "second",
	DtDatetime:      "datetime",
	DtTimestamp:     "timestamp",
	DtNanoTime:      "nanotime",
	DtNanoTimestamp: "nanotimestamp",
	DtFloat:         "float",
	DtDouble:        "double",
	DtSymbol:        "symbol",
	DtString:        "string",
	DtUUID:          "uuid",
	DtFunction:      "function",
	DtHandle:        "handle",
	DtCode:          "code",
	DtDatasource:    "datasource",
	DtResource:      "resource",
	DtAny:           "any",
	DtCompress:      "compress",
	DtDictionary:    "dictionary",
	DtDateHour:      "dateHour",
	DtDateMinute:    "dateMinute",
	DtIP:            "IP",
	DtInt128:        "int128",
	DtBlob:          "blob",
	dt33:            "Dt33",
	DtComplex:       "complex",
	DtPoint:         "point",
	DtDuration:      "duration",
	DtObject:        "object",
}

var dataFormStringMap = map[DataFormByte]string{
	DfChart:      "chart",
	DfChunk:      "chunk",
	DfDictionary: "dictionary",
	DfMatrix:     "matrix",
	DfPair:       "pair",
	DfScalar:     "scalar",
	DfSet:        "set",
	DfTable:      "table",
	DfVector:     "vector",
}

var durationUnit = map[uint32]string{
	0:  "ns",
	1:  "us",
	2:  "ms",
	3:  "s",
	4:  "m",
	5:  "H",
	6:  "d",
	7:  "w",
	8:  "M",
	9:  "y",
	10: "B",
}

var durationUnitReverse = map[string]uint32{
	"ns": 0,
	"us": 1,
	"ms": 2,
	"s":  3,
	"m":  4,
	"H":  5,
	"d":  6,
	"w":  7,
	"M":  8,
	"y":  9,
	"B":  10,
}

// GetDataTypeString returns the data type in the string format based on its byte format.
func GetDataTypeString(t DataTypeByte) string {
	dts := ""
	if t > 128 {
		return "symbolExtend"
	} else if t > 64 {
		t -= 64
		dts = "Array"
	}

	dts = dataTypeStringMap[t] + dts
	return dts
}

// GetDataFormString returns the data form in the string format based on its byte format.
func GetDataFormString(t DataFormByte) string {
	return dataFormStringMap[t]
}

// GetCategory returns the category string according to the dt.
func GetCategory(d DataTypeByte) CategoryString {
	if d > 128 {
		d -= 128
	} else if d > 64 {
		d -= 64
	}

	switch {
	case d == DtTime || d == DtSecond || d == DtMinute || d == DtDate || d == DtDatetime ||
		d == DtMonth || d == DtTimestamp || d == DtNanoTime || d == DtNanoTimestamp ||
		d == DtDateHour || d == DtDateMinute:
		return TEMPORAL
	case d == DtInt || d == DtLong || d == DtShort || d == DtChar:
		return INTEGRAL
	case d == DtBool:
		return LOGICAL
	case d == DtFloat || d == DtDouble:
		return FLOATING
	case d == DtString || d == DtSymbol:
		return LITERAL
	case d == DtInt128 || d == DtUUID || d == DtIP:
		return BINARY
	case d == DtAny:
		return MIXED
	case d == DtVoid:
		return NOTHING
	default:
		return SYSTEM
	}
}

// CastDateTime casts src to other DataForm according to the dt.
func CastDateTime(src DataForm, d DataTypeByte) (DataForm, error) {
	switch src.GetDataForm() {
	case DfScalar:
		return castScalarDateTime(src.(*Scalar), d)
	case DfVector:
		return castVectorDateTime(src.(*Vector), d)
	default:
		return nil, errors.New("the source data must be a temporal scalar/vector")
	}
}

func castScalarDateTime(src *Scalar, dtb DataTypeByte) (*Scalar, error) {
	res, err := castDateTypeDateTime(src.DataType)
	if err != nil {
		return nil, err
	}

	dt, err := NewDataType(dtb, res)
	if err != nil {
		return nil, err
	}

	return NewScalar(dt), nil
}

func castVectorDateTime(src *Vector, dtb DataTypeByte) (*Vector, error) {
	rows := src.Rows()
	dtl := make([]int32, rows)
	for i := 0; i < rows; i++ {
		raw := src.Data.Get(i)
		if raw.DataType() == DtAny {
			sca := raw.Value().(*Scalar)
			raw = sca.DataType
		}

		t, err := castDateTypeDateTime(raw)
		if err != nil {
			return nil, err
		}

		switch dtb {
		case DtMonth:
			dtl[i] = renderMonthFromTime(t)
		case DtDate:
			dtl[i] = renderDateFromTime(t)
		case DtDateHour:
			dtl[i] = renderDateHourFromTime(t)
		case DtTime:
			dtl[i] = renderTimeFromTime(t)
		default:
			return nil, errors.New("failed to cast vector of DateTime type")
		}
	}

	l := &dataTypeList{
		t:       dtb,
		bo:      protocol.LittleEndian,
		count:   rows,
		intData: dtl,
	}

	return NewVector(l), nil
}

func castDateTypeDateTime(raw DataType) (time.Time, error) {
	dt := raw.(*dataType)

	var t time.Time
	switch dt.DataType() {
	case DtNanoTimestamp:
		res := dt.data.(int64)
		t = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(res) * time.Nanosecond)
	case DtTimestamp:
		res := dt.data.(int64)
		t = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(res) * time.Millisecond)
	case DtDatetime:
		res := dt.data.(int32)
		t = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(res) * time.Second)
	case DtDate:
		res := dt.data.(int32)
		t = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(res*24) * time.Hour)
	default:
		return time.Time{}, errors.New("the data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATE or DATETIME")
	}

	return t, nil
}

func read2Uint32(r protocol.Reader, bo protocol.ByteOrder) (uint32, uint32, error) {
	bs, err := r.ReadCertainBytes(8)
	if err != nil {
		return 0, 0, err
	}

	return bo.Uint32(bs), bo.Uint32(bs[4:]), nil
}

func read2Uint16(r protocol.Reader, bo protocol.ByteOrder) (uint16, uint16, error) {
	bs, err := r.ReadCertainBytes(4)
	if err != nil {
		return 0, 0, err
	}

	return bo.Uint16(bs), bo.Uint16(bs[2:]), nil
}

func stringToUint64(s string) uint64 {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0
	}

	return v
}

func contains(raw []string, s string) (int, bool) {
	for k, v := range raw {
		if v == s {
			return k, true
		}
	}

	return 0, false
}
