package model

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/shopspring/decimal"
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
	DtDateHour:      "datehour",
	DtDateMinute:    "dateminute",
	DtIP:            "ipaddr",
	DtInt128:        "int128",
	DtBlob:          "blob",
	dt33:            "Dt33",
	DtComplex:       "complex",
	DtPoint:         "point",
	DtDuration:      "duration",
	DtDecimal32:     "decimal32",
	DtDecimal64:     "decimal64",
	DtObject:        "object",
}

var dataTypeByteMap = map[string]DataTypeByte{
	"void":          DtVoid,
	"bool":          DtBool,
	"char":          DtChar,
	"short":         DtShort,
	"int":           DtInt,
	"long":          DtLong,
	"date":          DtDate,
	"month":         DtMonth,
	"time":          DtTime,
	"minute":        DtMinute,
	"second":        DtSecond,
	"datetime":      DtDatetime,
	"timestamp":     DtTimestamp,
	"nanotime":      DtNanoTime,
	"nanotimestamp": DtNanoTimestamp,
	"float":         DtFloat,
	"double":        DtDouble,
	"symbol":        DtSymbol,
	"string":        DtString,
	"uuid":          DtUUID,
	"function":      DtFunction,
	"handle":        DtHandle,
	"code":          DtCode,
	"datasource":    DtDatasource,
	"resource":      DtResource,
	"any":           DtAny,
	"compress":      DtCompress,
	"dictionary":    DtDictionary,
	"datehour":      DtDateHour,
	"dateminute":    DtDateMinute,
	"ipaddr":        DtIP,
	"int128":        DtInt128,
	"blob":          DtBlob,
	"Dt33":          dt33,
	"complex":       DtComplex,
	"point":         DtPoint,
	"duration":      DtDuration,
	"decimal32":     DtDecimal32,
	"decimal64":     DtDecimal64,
	"object":        DtObject,
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

func parseTags(raw string) map[string]string {
	res := make(map[string]string)
	strs := strings.Split(raw, ";")
	for _, v := range strs {
		if !strings.Contains(v, ":") {
			continue
		}
		rawTag := strings.Split(v, ":")
		res[strings.TrimSpace(rawTag[0])] = strings.TrimSpace(rawTag[1])
	}

	return res
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
	case d == DtDecimal32, d == DtDecimal64:
		return DENARY
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

func stringsToUint64(s []string, bo protocol.ByteOrder) uint64 {
	res := make([]byte, 8)
	for k, v := range s {
		u16, err := strconv.ParseUint(v, 16, 16)
		if err != nil {
			return 0
		}

		bo.PutUint16(res[6-2*k:], uint16(u16))
	}

	return bo.Uint64(res)
}

func contains(raw []string, s string) (int, bool) {
	for k, v := range raw {
		if v == s {
			return k, true
		}
	}

	return 0, false
}

func calculateDecimal32(scale int32, value float64) (float64, error) {
	if value == NullDecimal32Value {
		return value, nil
	}
	d1 := decimal.NewFromFloat(value)
	d2 := decimal.NewFromFloat(math.Pow10(int(scale)))
	res := d1.Mul(d2)
	f, _ := res.Float64()
	if f < NullDecimal32Value || f > maxDecimal32Value {
		return 0, errors.New("Decimal math overflow")
	}

	return f, nil
}

func calculateDecimal64(scale int32, value float64) (float64, error) {
	if value == NullDecimal64Value {
		return value, nil
	}
	d1 := decimal.NewFromFloat(value)
	d2 := decimal.NewFromFloat(math.Pow10(int(scale)))
	res := d1.Mul(d2)
	f, _ := res.Float64()
	if f < NullDecimal64Value || f > maxDecimal64Value {
		return 0, errors.New("Decimal math overflow")
	}

	return f, nil
}
