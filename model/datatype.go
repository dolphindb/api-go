package model

import (
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/shopspring/decimal"
)

// DataType interface declares functions to handle DataType data.
type DataType interface {
	// DataType returns the byte type of DataType
	DataType() DataTypeByte
	// Render serializes the DataType into w with the bo
	Render(w *protocol.Writer, bo protocol.ByteOrder) error

	// String returns the string format of the DataType
	String() string
	// Bool returns the bool value of the DataType.
	// Only when th DataType is DtBool, you can call the func successful
	Bool() (bool, error)

	// Value returns an interface value of DataType, you can assert it to get the real value
	Value() interface{}

	// HashBucket calculates the hashcode with the value of DataType and buckets
	HashBucket(buckets int) int

	// IsNull checks whether the value of DataType is null
	IsNull() bool
	// SetNull sets the value of DataType to null
	SetNull()

	raw() interface{}
}

type dataType struct {
	t    DataTypeByte
	data interface{}

	bo protocol.ByteOrder
}

type decimal128Data struct {
	scale int32
	value *big.Int
}

type Decimal64 struct {
	Scale int32
	Value float64
}

type Decimal32 struct {
	Scale int32
	Value float64
}

type Decimal128 struct {
	Scale int32
	Value string
}

var nullDataForm = &Scalar{
	category: &Category{
		DataForm: DfScalar,
		DataType: DtVoid,
	},
	DataType: &dataType{
		t:    DtVoid,
		bo:   protocol.LittleEndian,
		data: byte(0),
	},
}

// NewDataType returns an object of DataType according to datatype and arg.
// You should input in according to the datatype.
// See README.md for more details.
func NewDataType(datatype DataTypeByte, arg interface{}) (DataType, error) {
	if datatype > 128 {
		datatype -= 128
	} else if datatype > 64 {
		datatype -= 64
	}

	dt := &dataType{
		t:  datatype,
		bo: protocol.LittleEndian,
	}

	if arg == nil {
		dt.SetNull()
		return dt, nil
	}

	err := dt.renderData(arg)
	return dt, err
}

func (d *dataType) renderData(in interface{}) error {
	var err error
	switch d.t {
	case DtVoid:
		d.data = byte(0)
	case DtBool:
		d.data, err = renderBool(in)
	case DtBlob:
		d.data, err = renderBlob(in)
	case DtChar, DtCompress:
		d.data, err = renderByte(in)
	case DtComplex, DtPoint:
		d.data, err = renderDouble2(in)
	case DtDate:
		d.data, err = renderDate(in)
	case DtDateHour:
		d.data, err = renderDateHour(in)
	case DtDatetime:
		d.data, err = renderDateTime(in)
	case DtDateMinute:
		d.data, err = renderDateMinute(in)
	case DtDouble:
		d.data, err = renderDouble(in)
	case DtFloat:
		d.data, err = renderFloat(in)
	case DtDuration:
		d.data, err = renderDuration(in)
	case DtInt:
		d.data, err = renderInt(in)
	case DtInt128:
		d.data, err = renderInt128(in)
	case DtIP:
		d.data, err = renderIP(in, d.bo)
	case DtDecimal32:
		d.data, err = renderDecimal32(in)
	case DtDecimal64:
		d.data, err = renderDecimal64(in)
	case DtDecimal128:
		d.data, err = renderDecimal128(in)
	case DtLong:
		d.data, err = renderLong(in)
	case DtMinute:
		d.data, err = renderMinute(in)
	case DtMonth:
		d.data, err = renderMonth(in)
	case DtNanoTime:
		d.data, err = renderNanoTime(in)
	case DtNanoTimestamp:
		d.data, err = renderNanoTimestamp(in)
	case DtSecond:
		d.data, err = renderSecond(in)
	case DtShort:
		d.data, err = renderShort(in)
	case DtTime:
		d.data, err = renderTime(in)
	case DtTimestamp:
		d.data, err = renderTimestamp(in)
	case DtUUID:
		d.data, err = renderUUID(in)
	case DtAny:
		d.data, err = renderAny(in)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.data, err = renderString(in)
	}

	return err
}

func (d *dataType) DataType() DataTypeByte {
	return d.t
}

func (d *dataType) Bool() (bool, error) {
	if d.t != DtBool {
		return false, fmt.Errorf("Bool() is invalid for DataType %s", GetDataTypeString(d.t))
	}

	return d.data.(uint8) == 1, nil
}

func (d *dataType) raw() interface{} {
	return d.data
}

func (d *dataType) SetNull() {
	switch d.t {
	case DtVoid:
		d.data = byte(0)
	case DtBool, DtChar, DtCompress:
		d.data = NullBool
	case DtBlob:
		d.data = NullBlob
	case DtComplex, DtPoint:
		d.data = NullPoint
	case DtDate, DtDateHour, DtDatetime, DtInt, DtMinute, DtMonth, DtSecond, DtTime:
		d.data = NullInt
	case DtDouble:
		d.data = NullDouble
	case DtFloat:
		d.data = NullFloat
	case DtDuration:
		d.data = emptyDuration
	case DtNanoTime, DtNanoTimestamp, DtLong, DtTimestamp:
		d.data = NullLong
	case DtShort:
		d.data = NullShort
	case DtUUID, DtInt128, DtIP:
		d.data = emptyInt64List
	case DtDecimal32:
		d.data = [2]int32{0, NullInt}
	case DtDecimal64:
		d.data = [2]int64{0, NullLong}
	case DtDecimal128:
		d.data = decimal128Data{scale: 0, value: minBigIntValue}
	case DtAny:
		d.data = nullDataForm
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.data = NullString
	}
}

func (d *dataType) IsNull() bool {
	if d.t == DtVoid {
		return true
	}

	switch d.t {
	case DtVoid:
		return true
	case DtDecimal32:
		t := d.data.([2]int32)
		return t[1] == NullInt
	case DtDecimal64:
		t := d.data.([2]int64)
		return t[1] == NullLong
	case DtDecimal128:
		t := d.data.(decimal128Data)
		return t.value.Cmp(minBigIntValue) == 0
	}

	res := false
	val := d.Value()
	switch r := val.(type) {
	case time.Time:
		res = r == NullTime
	case int32:
		res = r == NullInt
	case []byte:
		res = len(r) == 0
	case int16:
		res = r == NullShort
	case int8:
		res = r == int8(math.MinInt8)
	case int64:
		res = r == NullLong
	case float32:
		res = r == NullFloat
	case float64:
		res = r == NullDouble
	case DataForm:
		res = r == nil || (r.GetDataForm() == DfScalar && r.GetDataType() == DtVoid)
	case string:
		switch d.t {
		case DtIP:
			res = r == NullIP
		case DtPoint:
			res = r == emptyPoint
		case DtInt128:
			res = r == NullInt128
		case DtUUID:
			res = r == NullUUID
		default:
			res = r == NullString
		}
	}
	return res
}

func (d *dataType) String() string {
	if d.IsNull() && d.t != DtUUID && d.t != DtIP && d.t != DtPoint && d.t != DtInt128 {
		return ""
	}

	res := d.Value()
	t1, ok := res.(time.Time)
	if ok {
		switch d.t {
		case DtDate:
			res = t1.Format("2006.01.02")
		case DtDateHour:
			res = t1.Format("2006.01.02T15")
		case DtDatetime:
			res = t1.Format("2006.01.02T15:04:05")
		case DtDateMinute:
			res = t1.Format("2006.01.02T15:04")
		case DtMinute:
			res = t1.Format("15:04m")
		case DtMonth:
			res = t1.Format("2006.01M")
		case DtNanoTime:
			res = t1.Format("15:04:05.000000000")
		case DtNanoTimestamp:
			res = t1.Format("2006.01.02T15:04:05.000000000")
		case DtSecond:
			res = t1.Format("15:04:05")
		case DtTime:
			res = t1.Format("15:04:05.000")
		case DtTimestamp:
			res = t1.Format("2006.01.02T15:04:05.000")
		}
	}

	switch d.t {
	case DtBlob:
		return fmt.Sprintf("%s", res)
	case DtDecimal32:
		r := res.(*Decimal32)
		f := decimal.NewFromFloat(r.Value)
		return f.StringFixed(r.Scale)
	case DtDecimal64:
		r := res.(*Decimal64)
		f := decimal.NewFromFloat(r.Value)
		return f.StringFixed(r.Scale)
	case DtDecimal128:
		r := res.(*Decimal128)
		dec, _ := decimal.NewFromString(r.Value)
		return dec.StringFixed(r.Scale)
	case DtFloat, DtDouble:
		return floatString(res)
	case DtInt, DtShort, DtLong:
		return fmt.Sprintf("%d", res)
	default:
		return fmt.Sprintf("%v", res)
	}
}

func (d *dataType) Value() interface{} {
	return value(d.t, d.data, d.bo)
}

func value(dt DataTypeByte, raw interface{}, bo protocol.ByteOrder) interface{} {
	var res interface{}
	switch dt {
	case DtVoid:
		res = void
	case DtBool:
		byt := raw.(byte)
		if byt == MinInt8 {
			res = int8(-128)
		} else {
			res = byt == 1
		}
	case DtBlob:
		res = raw.([]byte)
	case DtChar, DtCompress:
		res = int8(raw.(byte))
	case DtComplex:
		res = parseComplex(raw)
	case DtDate:
		res = parseDate(raw)
	case DtDateMinute:
		res = parseDateMinute(raw)
	case DtDateHour:
		res = parseDateHour(raw)
	case DtDatetime:
		res = parseDateTime(raw)
	case DtDouble:
		res = raw.(float64)
	case DtFloat:
		res = raw.(float32)
	case DtDuration:
		res = parseDuration(raw)
	case DtInt:
		res = raw.(int32)
	case DtInt128:
		res = parseInt128(raw)
	case DtIP:
		res = parseIP(raw, bo)
	case DtDecimal32:
		t := raw.([2]int32)
		res = decimal32(t[0], t[1])
	case DtDecimal64:
		t := raw.([2]int64)
		res = decimal64(t[0], t[1])
	case DtDecimal128:
		t := raw.(decimal128Data)
		res = decimal128(t.scale, t.value)
	case DtLong:
		res = raw.(int64)
	case DtMinute:
		res = parseMinute(raw)
	case DtAny:
		res = raw.(DataForm)
	case DtMonth:
		res = parseMonth(raw)
	case DtNanoTime:
		res = parseNanoTime(raw)
	case DtNanoTimestamp:
		res = parseNanoTimeStamp(raw)
	case DtPoint:
		res = parsePoint(raw)
	case DtSecond:
		res = parseSecond(raw)
	case DtShort:
		res = raw.(int16)
	case DtTime:
		res = parseTime(raw)
	case DtTimestamp:
		res = parseTimeStamp(raw)
	case DtUUID:
		res = parseUUID(raw, bo)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res = raw.(string)
	}

	return res
}

func (d *dataType) HashBucket(buckets int) int {
	switch d.t {
	case DtDuration:
		return 0
	case DtFloat, DtComplex, DtPoint:
		return -1
	case DtInt, DtDate, DtTime, DtMonth, DtMinute, DtSecond, DtDateHour, DtDatetime:
		return d.intHashBucket(buckets)
	case DtInt128, DtIP, DtUUID:
		return d.int128HashBucket(buckets)
	case DtChar:
		return d.charHashBucket(buckets)
	case DtShort:
		return d.shortHashBucket(buckets)
	case DtLong, DtNanoTime, DtNanoTimestamp, DtTimestamp:
		return d.longHashBucket(buckets)
	case DtString, DtSymbol:
		return d.stringHashBucket(buckets)
	default:
		return 0
	}
}

func (d *dataType) stringHashBucket(buckets int) int {
	val := d.data.(string)

	byteCount := 0
	rs := []rune(val)
	for _, c := range rs {
		switch {
		case c >= '\u0001' && c <= '\u007f':
			byteCount++
		case c == '\u0000' || c <= '\u07ff':
			byteCount += 2
		default:
			byteCount += 3
		}
	}

	h := uint32(byteCount)
	if byteCount == len(rs) {
		h = d.hashNormalChar(byteCount, h, rs)
	} else {
		h = d.hashSpecialChar(val, h)
	}

	return int(h) % buckets
}

func (d *dataType) hashNormalChar(byteCount int, h uint32, rs []rune) uint32 {
	l := byteCount / 4
	for i := 0; i < l; i++ {
		offSet := i * 4
		k := uint32((rs[offSet] & 0xff) + ((rs[offSet+1] & 0xff) << 8) +
			((rs[offSet+2] & 0xff) << 16) + ((rs[offSet+3] & 0xff) << 24))
		k *= 0x5bd1e995
		k ^= k >> 24
		k *= 0x5bd1e995
		h *= 0x5bd1e995
		h ^= k
	}

	switch byteCount % 4 {
	case 3:
		h ^= uint32((rs[byteCount&^3+2] & 0xff) << 16)
		fallthrough
	case 2:
		h ^= uint32((rs[byteCount&^3+1] & 0xff) << 8)
		fallthrough
	case 1:
		h ^= uint32(rs[byteCount&^3] & 0xff)
		h *= 0x5bd1e995
	}

	h ^= h >> 13
	h *= 0x5bd1e995
	h ^= h >> 15

	return h
}

func (d *dataType) hashSpecialChar(val string, h uint32) uint32 {
	k := uint32(0)
	cursor := 0
	for _, c := range val {
		switch {
		case c >= '\u0001' && c <= '\u007f':
			k += uint32(c << (8 * cursor))
			cursor++
		case c == '\u0000' || c <= '\u07ff':
			k += uint32((0xc0 | (0x1f & (c >> 6))) << (8 * cursor))
			cursor++
			if cursor == 4 {
				h = specificBitCalculate(k, h)
				k = 0
				cursor = 0
			}

			k += uint32((0x80 | (0x3f & c)) << (8 * cursor))
			cursor++
		default:
			k += uint32((0xe0 | (0x0f & (c >> 12))) << (8 * cursor))
			cursor++
			if cursor == 4 {
				h = specificBitCalculate(k, h)
				k = 0
				cursor = 0
			}
			k += uint32((0x80 | (0x3f & (c >> 6))) << (8 * cursor))
			cursor++
			if cursor == 4 {
				h = specificBitCalculate(k, h)
				k = 0
				cursor = 0
			}
			k += uint32((0x80 | (0x3f & c)) << (8 * cursor))
			cursor++
		}

		if cursor == 4 {
			h = specificBitCalculate(k, h)
			k = 0
			cursor = 0
		}
	}

	if cursor > 0 {
		h ^= k
		h *= 0x5bd1e995
	}

	h ^= h >> 13
	h *= 0x5bd1e995
	h ^= h >> 15

	return h
}

func specificBitCalculate(k, h uint32) uint32 {
	k *= 0x5bd1e995
	k ^= k >> 24
	k *= 0x5bd1e995
	h *= 0x5bd1e995
	h ^= k

	return h
}

func (d *dataType) longHashBucket(buckets int) int {
	value := d.data.(int64)
	switch {
	case value >= 0:
		return int(value % int64(buckets))
	case value == math.MinInt64:
		return -1
	default:
		return ((math.MaxInt64 % buckets) + 2 + ((math.MaxInt64 + int(value)) % buckets)) % buckets
	}
}

func (d *dataType) int128HashBucket(buckets int) int {
	p := d.data.([2]uint64)
	m := 0x5bd1e995
	r := 24
	h := uint32(16)

	k1 := uint32(p[0] & math.MaxUint32)
	k2 := uint32(p[0] >> 32)
	k3 := uint32(p[1] & math.MaxUint32)
	k4 := uint32(p[1] >> 32)

	k1 *= uint32(m)
	k1 ^= k1 >> r
	k1 *= uint32(m)

	k2 *= uint32(m)
	k2 ^= k2 >> r
	k2 *= uint32(m)

	k3 *= uint32(m)
	k3 ^= k3 >> r
	k3 *= uint32(m)

	k4 *= uint32(m)
	k4 ^= k4 >> r
	k4 *= uint32(m)

	h *= uint32(m)
	h ^= k1
	h *= uint32(m)
	h ^= k2
	h *= uint32(m)
	h ^= k3
	h *= uint32(m)
	h ^= k4

	h ^= h >> 13
	h *= uint32(m)
	h ^= h >> 15

	return int(h) % buckets
}

func (d *dataType) charHashBucket(buckets int) int {
	value := d.data.(uint8)
	r := int(int8(value))
	switch {
	case r >= 0:
		return r % buckets
	case r == math.MinInt8:
		return -1
	default:
		return (r + 4294967296) % buckets
	}
}

func (d *dataType) shortHashBucket(buckets int) int {
	value := d.data.(int16)
	switch {
	case value >= 0:
		return int(value) % buckets
	case value == math.MinInt16:
		return -1
	default:
		return (int(value) + 4294967296) % buckets
	}
}

func (d *dataType) intHashBucket(buckets int) int {
	value := d.data.(int32)
	switch {
	case value >= 0:
		return int(value) % buckets
	case value == math.MinInt32:
		return -1
	default:
		return (int(value) + 4294967296) % buckets
	}
}

func (d *Decimal64) String() string {
	f, err := calculateDecimal64(d.Scale, d.Value)
	if f == NullDecimal64Value || err != nil {
		return ""
	}

	return decimal.NewFromFloat(d.Value).StringFixed(d.Scale)
}

func (d *Decimal32) String() string {
	f, err := calculateDecimal32(d.Scale, d.Value)
	if f == NullDecimal32Value || err != nil {
		return ""
	}

	return decimal.NewFromFloat(d.Value).StringFixed(d.Scale)
}

func (d *Decimal128) String() string {
	f, err := calculateDecimal128(d.Scale, d.Value)
	if f.String() == NullDecimal128Value || err != nil {
		return ""
	}

	dec, err := decimal.NewFromString(d.Value)
	if err != nil {
		return ""
	}

	return dec.StringFixed(d.Scale)
}
