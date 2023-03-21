package model

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// DataTypeList interface declares functions to handle DataType list.
type DataTypeList interface {
	// DataType returns the byte type of the DataTypeList
	DataType() DataTypeByte
	// Render serializes the DataForm with bo and input it into w
	Render(w *protocol.Writer, bo protocol.ByteOrder) error

	// Len returns the length of DataTypeList
	Len() int
	// Get returns a DataType value in DataTypeList according to the ind
	// which must be less than len(DataTypeList)
	Get(ind int) DataType
	// Set inserts dt into DataTypeList according to the ind
	// which must be less than or equal to len(DataTypeList).
	// If ind < len(DataTypeList), cover the original value in DataTypeList
	Set(ind int, t DataType) error
	// Deprecated.
	// Use SetWithRawData instead.
	SetWithRaw(ind int, arg interface{}) error
	// SetWithRaw inserts raw data into DataTypeList according to the ind
	// which must be less than or equal to len(DataTypeList).
	// If ind < len(DataTypeList), cover the original value in DataTypeList
	// Refer to README.md for the valid type of arg.
	SetWithRawData(ind int, arg interface{}) error
	// Append inserts a DataType value to the end of DataTypeList.
	// The type of d must be the same as DataTypeList's
	Append(t DataType) DataTypeList
	// Sub returns the len end-st of DataTypeList.
	// End must be larger than st,
	// but less than the len of DataTypeList.
	// St must be large than -1
	Sub(st, end int) DataTypeList
	// AsOf returns the index of the d in DataTypeList.
	// If d is not in DataTypeList, returns -1
	AsOf(d DataType) int

	// StringList returns the string array of Datatype list
	StringList() []string

	// Value returns the value of Datatype list
	Value() []interface{}

	// IsNull checks whether the value of DataType is null based on the index
	IsNull(ind int) bool
	// SetNull sets the value of DataType with index ind to null
	SetNull(ind int)
	// GetSubList instantiates a DataTypeList with the values in indexes which is a list of index.
	// Value in indexes should be less than the length of DataTypeList
	GetSubList(indexes []int) DataTypeList
	// ElementValue returns the value of Datatype list element according to the ind
	ElementValue(ind int) interface{}
	// ElementString returns the string value of Datatype list element according to the ind
	ElementString(ind int) string

	combine(dtl DataTypeList) (DataTypeList, error)
}

type dataTypeList struct {
	count int
	t     DataTypeByte
	bo    protocol.ByteOrder

	shortData  []int16
	intData    []int32
	longData   []int64
	floatData  []float32
	doubleData []float64
	stringData []string
	charData   []uint8
	blobData   [][]byte

	anyData       []DataForm
	double2Data   []float64
	long2Data     []uint64
	decimal32Data []int32
	decimal64Data []int64
	durationData  []uint32
}

type Decimal64s struct {
	Scale int32
	Value []float64
}

type Decimal32s struct {
	Scale int32
	Value []float64
}

// NewDataTypeList instantiates a DataTypeList according to the datatype and data.
// The DataType byte of element in data should be equal to datatype.
func NewDataTypeList(datatype DataTypeByte, data []DataType) DataTypeList {
	size := len(data)
	res := &dataTypeList{
		count: size,
		t:     datatype,
		bo:    protocol.LittleEndian,
	}

	switch datatype {
	case DtVoid, DtBool, DtChar:
		res.charData = make([]uint8, size)
		for k, v := range data {
			res.charData[k] = v.raw().(uint8)
		}
	case DtShort:
		res.shortData = make([]int16, size)
		for k, v := range data {
			res.shortData[k] = v.raw().(int16)
		}
	case DtFloat:
		res.floatData = make([]float32, size)
		for k, v := range data {
			res.floatData[k] = v.raw().(float32)
		}
	case DtDouble:
		res.doubleData = make([]float64, size)
		for k, v := range data {
			res.doubleData[k] = v.raw().(float64)
		}
	case DtDuration:
		res.durationData = make([]uint32, 0, 2*size)
		for _, v := range data {
			tmp := v.raw().([2]uint32)
			res.durationData = append(res.durationData, tmp[0], tmp[1])
		}
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		res.intData = make([]int32, size)
		for k, v := range data {
			res.intData[k] = v.raw().(int32)
		}
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		res.longData = make([]int64, size)
		for k, v := range data {
			res.longData[k] = v.raw().(int64)
		}
	case DtInt128, DtIP, DtUUID:
		res.long2Data = make([]uint64, 0, 2*size)
		for _, v := range data {
			tmp := v.raw().([2]uint64)
			res.long2Data = append(res.long2Data, tmp[0], tmp[1])
		}
	case DtDecimal32:
		res.decimal32Data = make([]int32, 0, size+1)
		for k, v := range data {
			tmp := v.raw().([2]int32)
			if k == 0 {
				res.decimal32Data = append(res.decimal32Data, tmp[0], tmp[1])
			} else {
				res.decimal32Data = append(res.decimal32Data, tmp[1])
			}
		}
	case DtDecimal64:
		res.decimal64Data = make([]int64, 0, size+1)
		for k, v := range data {
			tmp := v.raw().([2]int64)
			if k == 0 {
				res.decimal64Data = append(res.decimal64Data, tmp[0], tmp[1])
			} else {
				res.decimal64Data = append(res.decimal64Data, tmp[1])
			}
		}
	case DtComplex, DtPoint:
		res.double2Data = make([]float64, 0, 2*size)
		for _, v := range data {
			tmp := v.raw().([2]float64)
			res.double2Data = append(res.double2Data, tmp[0], tmp[1])
		}
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res.stringData = make([]string, size)
		for k, v := range data {
			res.stringData[k] = v.raw().(string)
		}
	case DtBlob:
		res.blobData = make([][]byte, size)
		for k, v := range data {
			res.blobData[k] = v.raw().([]byte)
		}
	case DtAny:
		res.anyData = make([]DataForm, size)
		for k, v := range data {
			res.anyData[k] = v.raw().(DataForm)
		}
	}

	return res
}

// NewEmptyDataTypeList instantiates an empty DataTypeList.
func NewEmptyDataTypeList(datatype DataTypeByte, size int) DataTypeList {
	if datatype > 128 {
		datatype -= 128
	} else if datatype > 64 {
		datatype -= 64
	}

	res := &dataTypeList{
		count: size,
		t:     datatype,
		bo:    protocol.LittleEndian,
	}

	switch datatype {
	case DtVoid, DtBool, DtChar:
		res.charData = make([]uint8, size)
	case DtShort:
		res.shortData = make([]int16, size)
	case DtFloat:
		res.floatData = make([]float32, size)
	case DtDouble:
		res.doubleData = make([]float64, size)
	case DtDuration:
		res.durationData = make([]uint32, 2*size)
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		res.intData = make([]int32, size)
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		res.longData = make([]int64, size)
	case DtInt128, DtIP, DtUUID:
		res.long2Data = make([]uint64, 2*size)
	case DtDecimal32:
		res.decimal32Data = make([]int32, 1+size)
	case DtDecimal64:
		res.decimal64Data = make([]int64, 1+size)
	case DtComplex, DtPoint:
		res.double2Data = make([]float64, 2*size)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res.stringData = make([]string, size)
	case DtBlob:
		res.blobData = make([][]byte, size)
	case DtAny:
		res.anyData = make([]DataForm, size)
	}
	for i := 0; i < size; i++ {
		res.SetNull(i)
	}

	return res
}

// Deprecated.
// Use NewDataTypeListFromRawData instead.
func NewDataTypeListWithRaw(datatype DataTypeByte, args interface{}) (DataTypeList, error) {
	return NewDataTypeListFromRawData(datatype, args)
}

// NewDataTypeListFromRawData instantiates a DataTypeList with specified datatype and args.
// Refer to README_CN.md for the valid type of args.
func NewDataTypeListFromRawData(datatype DataTypeByte, args interface{}) (DataTypeList, error) {
	var err error

	if datatype > 128 {
		datatype -= 128
	} else if datatype > 64 {
		datatype -= 64
	}

	res := &dataTypeList{
		t:  datatype,
		bo: protocol.LittleEndian,
	}

	switch datatype {
	case DtBool:
		err = res.renderBool(args)
	case DtBlob:
		err = res.renderBlob(args)
	case DtChar, DtCompress:
		err = res.renderByte(args)
	case DtComplex, DtPoint:
		err = res.renderDouble2(args)
	case DtDate:
		err = res.renderDate(args)
	case DtDateMinute:
		err = res.renderDateMinute(args)
	case DtDateHour:
		err = res.renderDateHour(args)
	case DtDatetime:
		err = res.renderDateTime(args)
	case DtDouble:
		err = res.renderDouble(args)
	case DtFloat:
		err = res.renderFloat(args)
	case DtDuration:
		err = res.renderDuration(args)
	case DtInt:
		err = res.renderInt(args)
	case DtInt128:
		err = res.renderInt128(args)
	case DtIP:
		err = res.renderIP(args, res.bo)
	case DtDecimal32:
		err = res.renderDecimal32(args)
	case DtDecimal64:
		err = res.renderDecimal64(args)
	case DtLong:
		err = res.renderLong(args)
	case DtMinute:
		err = res.renderMinute(args)
	case DtMonth:
		err = res.renderMonth(args)
	case DtNanoTime:
		err = res.renderNanoTime(args)
	case DtNanoTimestamp:
		err = res.renderNanoTimestamp(args)
	case DtSecond:
		err = res.renderSecond(args)
	case DtShort:
		err = res.renderShort(args)
	case DtTime:
		err = res.renderTime(args)
	case DtTimestamp:
		err = res.renderTimestamp(args)
	case DtUUID:
		err = res.renderUUID(args)
	case DtAny:
		err = res.renderAny(args)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		err = res.renderString(args)
	default:
		return nil, fmt.Errorf("invalid DataType %d", datatype)
	}

	return res, err
}

func (d *dataTypeList) SetNull(ind int) {
	if ind >= d.count {
		return
	}

	switch d.t {
	case DtBool, DtChar, DtCompress:
		d.charData[ind] = NullBool
	case DtBlob:
		d.blobData[ind] = NullBlob
	case DtComplex, DtPoint:
		i := 2 * ind
		d.double2Data[i] = -math.MaxFloat64
		d.double2Data[i+1] = -math.MaxFloat64
	case DtDate, DtDateHour, DtDateMinute, DtDatetime, DtInt, DtMinute, DtMonth, DtSecond, DtTime:
		d.intData[ind] = NullInt
	case DtDouble:
		d.doubleData[ind] = NullDouble
	case DtFloat:
		d.floatData[ind] = NullFloat
	case DtDuration:
		i := 2 * ind
		d.durationData[i] = MinInt32
		d.durationData[i+1] = 0
	case DtNanoTime, DtNanoTimestamp, DtLong, DtTimestamp:
		d.longData[ind] = NullLong
	case DtShort:
		d.shortData[ind] = NullShort
	case DtUUID, DtInt128, DtIP:
		i := 2 * ind
		d.long2Data[i] = 0
		d.long2Data[i+1] = 0
	case DtDecimal32:
		d.decimal32Data[ind+1] = NullInt
	case DtDecimal64:
		d.decimal64Data[ind+1] = NullLong
	case DtAny:
		d.anyData[ind] = nullDataForm
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.stringData[ind] = NullString
	}
}

func (d *dataTypeList) ElementValue(ind int) interface{} {
	if ind >= d.count {
		return nil
	}

	var res interface{}
	switch d.t {
	case DtVoid:
		res = void
	case DtBool:
		byt := d.charData[ind]
		if byt == MinInt8 {
			res = int8(-128)
		} else {
			res = byt == 1
		}
	case DtBlob:
		res = d.blobData[ind]
	case DtChar, DtCompress:
		res = int8(d.charData[ind])
	case DtComplex:
		i := 2 * ind
		res = parseComplex([2]float64{d.double2Data[i], d.double2Data[i+1]})
	case DtPoint:
		i := 2 * ind
		res = parsePoint([2]float64{d.double2Data[i], d.double2Data[i+1]})
	case DtDate:
		res = parseDate(d.intData[ind])
	case DtDateMinute:
		res = parseDateMinute(d.intData[ind])
	case DtDateHour:
		res = parseDateHour(d.intData[ind])
	case DtDatetime:
		res = parseDateTime(d.intData[ind])
	case DtDouble:
		res = d.doubleData[ind]
	case DtFloat:
		res = d.floatData[ind]
	case DtDuration:
		i := 2 * ind
		res = parseDuration([2]uint32{d.durationData[i], d.durationData[i+1]})
	case DtInt:
		res = d.intData[ind]
	case DtInt128:
		i := 2 * ind
		res = parseInt128([2]uint64{d.long2Data[i], d.long2Data[i+1]})
	case DtIP:
		i := 2 * ind
		res = parseIP([2]uint64{d.long2Data[i], d.long2Data[i+1]}, d.bo)
	case DtDecimal32:
		res = decimal32(d.decimal32Data[0], d.decimal32Data[ind+1])
	case DtDecimal64:
		res = decimal64(d.decimal64Data[0], d.decimal64Data[ind+1])
	case DtLong:
		res = d.longData[ind]
	case DtMinute:
		res = parseMinute(d.intData[ind])
	case DtAny:
		res = d.anyData[ind]
	case DtMonth:
		res = parseMonth(d.intData[ind])
	case DtNanoTime:
		res = parseNanoTime(d.longData[ind])
	case DtNanoTimestamp:
		res = parseNanoTimeStamp(d.longData[ind])
	case DtSecond:
		res = parseSecond(d.intData[ind])
	case DtShort:
		res = d.shortData[ind]
	case DtTime:
		res = parseTime(d.intData[ind])
	case DtTimestamp:
		res = parseTimeStamp(d.longData[ind])
	case DtUUID:
		i := 2 * ind
		res = parseUUID([2]uint64{d.long2Data[i], d.long2Data[i+1]}, d.bo)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res = d.stringData[ind]
	}

	return res
}

func (d *dataTypeList) ElementString(ind int) string {
	if ind >= d.count {
		return ""
	}

	if d.IsNull(ind) && d.t != DtUUID && d.t != DtIP &&
		d.t != DtPoint && d.t != DtInt128 {
		return ""
	}

	raw := d.ElementValue(ind)
	if d.t == DtDate || d.t == DtDateHour || d.t == DtDatetime || d.t == DtDateMinute || d.t == DtMinute || d.t == DtMonth ||
		d.t == DtNanoTime || d.t == DtNanoTimestamp || d.t == DtTime || d.t == DtTimestamp ||
		d.t == DtSecond {
		times := []time.Time{raw.(time.Time)}
		return formatTime(d.t, times)[0]
	} else if d.t == DtBlob {
		return fmt.Sprintf("%s", raw)
	} else if d.t == DtDecimal32 {
		dec := raw.(*Decimal32)
		f := decimal.NewFromFloat(dec.Value)
		return f.StringFixed(dec.Scale)
	} else if d.t == DtDecimal64 {
		dec := raw.(*Decimal64)
		f := decimal.NewFromFloat(dec.Value)
		return f.StringFixed(dec.Scale)
	}

	return fmt.Sprintf("%v", raw)
}

func (d *dataTypeList) combine(in DataTypeList) (DataTypeList, error) {
	if d.t != in.DataType() {
		return nil, errors.New("the DataType must be the same when you call combine")
	}

	original := in.(*dataTypeList)
	res := &dataTypeList{
		bo:    d.bo,
		count: d.Len() + in.Len(),
		t:     d.t,
	}

	switch d.t {
	case DtBool, DtChar, DtCompress:
		res.charData = make([]uint8, res.count)
		copy(res.charData, d.charData)
		copy(res.charData[d.Len():], original.charData)
	case DtBlob:
		res.blobData = make([][]byte, res.count)
		copy(res.blobData, d.blobData)
		copy(res.blobData[d.Len():], original.blobData)
	case DtComplex, DtPoint:
		res.double2Data = make([]float64, res.count*2)
		copy(res.double2Data, d.double2Data)
		copy(res.double2Data[d.Len():], original.double2Data)
	case DtDate, DtDateHour, DtDateMinute, DtDatetime, DtInt, DtMinute, DtMonth, DtSecond, DtTime:
		res.intData = make([]int32, res.count)
		copy(res.intData, d.intData)
		copy(res.intData[d.Len():], original.intData)
	case DtDouble:
		res.doubleData = make([]float64, res.count)
		copy(res.doubleData, d.doubleData)
		copy(res.doubleData[d.Len():], original.doubleData)
	case DtFloat:
		res.floatData = make([]float32, res.count)
		copy(res.floatData, d.floatData)
		copy(res.floatData[d.Len():], original.floatData)
	case DtDuration:
		res.durationData = make([]uint32, res.count*2)
		copy(res.durationData, d.durationData)
		copy(res.durationData[d.Len():], original.durationData)
	case DtNanoTime, DtNanoTimestamp, DtLong, DtTimestamp:
		res.longData = make([]int64, res.count)
		copy(res.longData, d.longData)
		copy(res.longData[d.Len():], original.longData)
	case DtShort:
		res.shortData = make([]int16, res.count)
		copy(res.shortData, d.shortData)
		copy(res.shortData[d.Len():], original.shortData)
	case DtUUID, DtInt128, DtIP:
		res.long2Data = make([]uint64, res.count*2)
		copy(res.long2Data, d.long2Data)
		copy(res.long2Data[d.Len():], original.long2Data)
	case DtDecimal32:
		res.decimal32Data = make([]int32, res.count+1)
		res.decimal32Data[0] = d.decimal32Data[0]
		copy(res.decimal32Data[1:], d.decimal32Data)
		copy(res.decimal32Data[d.Len()+1:], original.decimal32Data)
	case DtDecimal64:
		res.decimal64Data = make([]int64, res.count+1)
		res.decimal64Data[0] = d.decimal64Data[0]
		copy(res.decimal64Data[1:], d.decimal64Data)
		copy(res.decimal64Data[d.Len()+1:], original.decimal64Data)
	case DtAny:
		res.anyData = make([]DataForm, res.count)
		copy(res.anyData, d.anyData)
		copy(res.anyData[d.Len():], original.anyData)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res.stringData = make([]string, res.count)
		copy(res.stringData, d.stringData)
		copy(res.stringData[d.Len():], original.stringData)
	}
	return res, nil
}

func (d *dataTypeList) IsNull(ind int) bool {
	if ind >= d.count {
		return true
	}

	res := false
	switch d.t {
	case DtVoid:
		res = true
	case DtBool, DtChar, DtCompress:
		res = d.charData[ind] == NullBool
	case DtBlob:
		res = len(d.blobData[ind]) == 0
	case DtComplex, DtPoint:
		i := 2 * ind
		res = d.double2Data[i] == -math.MaxFloat64 || d.double2Data[i+1] == -math.MaxFloat64
	case DtDate, DtDateHour, DtDateMinute, DtDatetime, DtInt, DtMinute, DtMonth, DtSecond, DtTime:
		res = d.intData[ind] == NullInt
	case DtDouble:
		res = d.doubleData[ind] == NullDouble
	case DtFloat:
		res = d.floatData[ind] == NullFloat
	case DtDuration:
		i := 2 * ind
		res = d.durationData[i] == MinInt32 && d.durationData[i+1] == 0
	case DtNanoTime, DtNanoTimestamp, DtLong, DtTimestamp:
		res = d.longData[ind] == NullLong
	case DtShort:
		res = d.shortData[ind] == NullShort
	case DtAny:
		df := d.anyData[ind]
		res = df == nil || (df.GetDataForm() == DfScalar && df.GetDataType() == DtVoid)
	case DtUUID, DtInt128, DtIP:
		i := 2 * ind
		res = d.long2Data[i] == 0 && d.long2Data[i+1] == 0
	case DtDecimal32:
		res = d.decimal32Data[ind+1] == NullInt
	case DtDecimal64:
		res = d.decimal64Data[ind+1] == NullLong
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res = d.stringData[ind] == NullString
	}

	return res
}

func (d *dataTypeList) Value() []interface{} {
	res := make([]interface{}, d.count)
	switch d.t {
	case DtVoid:
		for i := 0; i < d.count; i++ {
			res[i] = void
		}
	case DtBool:
		parseBools(d.charData, res)
	case DtBlob:
		parseBlobs(d.blobData, res)
	case DtChar, DtCompress:
		parseBytes(d.charData, res)
	case DtComplex:
		parseComplexes(d.count, d.double2Data, res)
	case DtPoint:
		parsePoints(d.count, d.double2Data, res)
	case DtDate:
		parseDates(d.intData, res)
	case DtDateMinute:
		parseDateMinutes(d.intData, res)
	case DtDateHour:
		parseDateHours(d.intData, res)
	case DtDatetime:
		parseDateTimes(d.intData, res)
	case DtDouble:
		parseDoubles(d.doubleData, res)
	case DtFloat:
		parseFloats(d.floatData, res)
	case DtDuration:
		parseDurations(d.count, d.durationData, res)
	case DtInt:
		parseInt(d.intData, res)
	case DtInt128:
		parseInt128s(d.count, d.long2Data, res)
	case DtAny:
		parseAny(d.anyData, res)
	case DtIP:
		parseIPs(d.count, d.long2Data, res, d.bo)
	case DtDecimal32:
		parseDecimal32s(d.count, d.decimal32Data, res)
	case DtDecimal64:
		parseDecimal64s(d.count, d.decimal64Data, res)
	case DtLong:
		parseLongs(d.longData, res)
	case DtMinute:
		parseMinutes(d.intData, res)
	case DtMonth:
		parseMonths(d.intData, res)
	case DtNanoTime:
		parseNanoTimes(d.longData, res)
	case DtNanoTimestamp:
		parseNanoTimeStamps(d.longData, res)
	case DtSecond:
		parseSeconds(d.intData, res)
	case DtShort:
		parseShorts(d.shortData, res)
	case DtTime:
		parseTimes(d.intData, res)
	case DtTimestamp:
		parseTimeStamps(d.longData, res)
	case DtUUID:
		parseUUIDs(d.count, d.long2Data, res, d.bo)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		parseStrings(d.stringData, res)
	}

	return res
}

func (d *dataTypeList) AsOf(t DataType) int {
	if d.t != t.DataType() {
		return -1
	}

	cg := GetCategory(d.DataType())
	if cg == MIXED || cg == NOTHING || cg == LOGICAL ||
		cg == SYSTEM || cg == BINARY {
		return -1
	}

	end := d.Len() - 1
	switch d.DataType() {
	case DtShort:
		end = d.shortAsOf(t)
	case DtInt, DtTime, DtSecond, DtMinute, DtDateHour, DtDate,
		DtDateMinute, DtDatetime, DtMonth:
		end = d.intAsOf(t)
	case DtLong, DtNanoTime, DtNanoTimestamp, DtTimestamp:
		end = d.longAsOf(t)
	case DtChar:
		end = d.charAsOf(t)
	case DtDouble:
		end = d.doubleAsOf(t)
	case DtFloat:
		end = d.floatAsOf(t)
	case DtString, DtSymbol:
		end = d.stringAsOf(t)
	}

	return end
}

func (d *dataTypeList) Set(ind int, t DataType) error {
	if d.count <= ind {
		return fmt.Errorf("index %d exceeds the number of data %d", ind, d.count)
	} else if t == nil {
		d.SetNull(ind)
		return nil
	}

	if !isEqualDataTypeByte(d.t, t.DataType()) {
		return fmt.Errorf("failed to set DataType(%s) into DataTypeList(%s)",
			GetDataTypeString(t.DataType()), GetDataTypeString(d.t))
	}

	return d.setWithRawData(ind, t.raw())
}

func isEqualDataTypeByte(a, b DataTypeByte) bool {
	if a == b || (a == DtSymbol && b == DtString) || (b == DtSymbol && a == DtString) {
		return true
	}

	return false
}

func (d *dataTypeList) setWithRawData(ind int, in interface{}) error {
	switch d.t {
	case DtBool:
		switch v := in.(type) {
		case byte:
			d.charData[ind] = v
		case bool:
			d.charData[ind] = boolToByte(v)
		default:
			return errors.New("the type of in must be byte or bool when datatype is DtBool")
		}
	case DtVoid:
		d.charData[ind] = byte(0)
	case DtChar:
		d.charData[ind] = in.(uint8)
	case DtShort:
		d.shortData[ind] = in.(int16)
	case DtFloat:
		d.floatData[ind] = in.(float32)
	case DtDouble:
		d.doubleData[ind] = in.(float64)
	case DtDuration:
		tmp := in.([2]uint32)
		i := 2 * ind
		d.durationData[i] = tmp[0]
		d.durationData[i+1] = tmp[1]
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		d.intData[ind] = in.(int32)
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		d.longData[ind] = in.(int64)
	case DtInt128, DtIP, DtUUID:
		tmp := in.([2]uint64)
		i := 2 * ind
		d.long2Data[i] = tmp[0]
		d.long2Data[i+1] = tmp[1]
	case DtDecimal32:
		tmp := in.([2]int32)
		d.decimal32Data[ind+1] = tmp[1]
	case DtDecimal64:
		tmp := in.([2]int64)
		d.decimal64Data[ind+1] = tmp[1]
	case DtComplex, DtPoint:
		tmp := in.([2]float64)
		i := 2 * ind
		d.double2Data[i] = tmp[0]
		d.double2Data[i+1] = tmp[1]
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.stringData[ind] = in.(string)
	case DtBlob:
		d.blobData[ind] = in.([]byte)
	case DtAny:
		d.anyData[ind] = in.(DataForm)
	}

	return nil
}

func (d *dataTypeList) SetWithRaw(ind int, arg interface{}) error {
	return d.SetWithRawData(ind, arg)
}

func (d *dataTypeList) SetWithRawData(ind int, arg interface{}) error {
	if d.count <= ind {
		return fmt.Errorf("index %d exceeds the number of data %d", ind, d.count)
	}

	var err error
	switch d.t {
	case DtVoid:
		d.charData[ind] = byte(0)
	case DtBool:
		d.charData[ind], err = renderBool(arg)
	case DtBlob:
		d.blobData[ind], err = renderBlob(arg)
	case DtChar, DtCompress:
		d.charData[ind], err = renderByte(arg)
	case DtComplex, DtPoint:
		err = d.SetDouble2(ind, arg)
	case DtDate:
		d.intData[ind], err = renderDate(arg)
	case DtDateHour:
		d.intData[ind], err = renderDateHour(arg)
	case DtDatetime:
		d.intData[ind], err = renderDateTime(arg)
	case DtDateMinute:
		d.intData[ind], err = renderDateMinute(arg)
	case DtDouble:
		d.doubleData[ind], err = renderDouble(arg)
	case DtFloat:
		d.floatData[ind], err = renderFloat(arg)
	case DtDuration:
		err = d.SetDuration(ind, arg)
	case DtInt:
		d.intData[ind], err = renderInt(arg)
	case DtInt128:
		err = d.SetInt128(ind, arg)
	case DtIP:
		err = d.SetIP(ind, arg)
	case DtDecimal32:
		err = d.SetDecimal32(ind, arg)
	case DtDecimal64:
		err = d.SetDecimal64(ind, arg)
	case DtLong:
		d.longData[ind], err = renderLong(arg)
	case DtMinute:
		d.intData[ind], err = renderMinute(arg)
	case DtMonth:
		d.intData[ind], err = renderMonth(arg)
	case DtNanoTime:
		d.longData[ind], err = renderNanoTime(arg)
	case DtNanoTimestamp:
		d.longData[ind], err = renderNanoTimestamp(arg)
	case DtSecond:
		d.intData[ind], err = renderSecond(arg)
	case DtShort:
		d.shortData[ind], err = renderShort(arg)
	case DtTime:
		d.intData[ind], err = renderTime(arg)
	case DtTimestamp:
		d.longData[ind], err = renderTimestamp(arg)
	case DtUUID:
		err = d.SetUUID(ind, arg)
	case DtAny:
		d.anyData[ind], err = renderAny(arg)
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.stringData[ind], err = renderString(arg)
	}

	return err
}

func (d *dataTypeList) SetUUID(ind int, in interface{}) error {
	tmp, err := renderUUID(in)
	if err != nil {
		return err
	}

	i := ind * 2
	d.long2Data[i] = tmp[0]
	d.long2Data[i+1] = tmp[1]
	return nil
}

func (d *dataTypeList) SetDuration(ind int, in interface{}) error {
	tmp, err := renderDuration(in)
	if err != nil {
		return err
	}

	i := ind * 2
	d.durationData[i] = tmp[0]
	d.durationData[i+1] = tmp[1]
	return nil
}

func (d *dataTypeList) SetDouble2(ind int, in interface{}) error {
	tmp, err := renderDouble2(in)
	if err != nil {
		return err
	}

	i := ind * 2
	d.double2Data[i] = tmp[0]
	d.double2Data[i+1] = tmp[1]
	return nil
}

func (d *dataTypeList) SetInt128(ind int, in interface{}) error {
	tmp, err := renderInt128(in)
	if err != nil {
		return err
	}

	i := ind * 2
	d.long2Data[i] = tmp[0]
	d.long2Data[i+1] = tmp[1]
	return nil
}

func (d *dataTypeList) SetDecimal32(ind int, in interface{}) error {
	tmp, err := renderDecimal32(in)
	if err != nil {
		return err
	}

	d.decimal32Data[ind+1] = tmp[1]
	return nil
}

func (d *dataTypeList) SetDecimal64(ind int, in interface{}) error {
	tmp, err := renderDecimal64(in)
	if err != nil {
		return err
	}

	d.decimal64Data[ind+1] = tmp[1]
	return nil
}

func (d *dataTypeList) SetIP(ind int, in interface{}) error {
	tmp, err := renderIP(in, d.bo)
	if err != nil {
		return err
	}

	i := ind * 2
	d.long2Data[i] = tmp[0]
	d.long2Data[i+1] = tmp[1]
	return nil
}

func (d *dataTypeList) Append(t DataType) DataTypeList {
	switch t.DataType() {
	case DtVoid, DtBool, DtChar:
		d.charData = append(d.charData, t.raw().(uint8))
	case DtShort:
		d.shortData = append(d.shortData, t.raw().(int16))
	case DtFloat:
		d.floatData = append(d.floatData, t.raw().(float32))
	case DtDouble:
		d.doubleData = append(d.doubleData, t.raw().(float64))
	case DtDuration:
		tmp := t.raw().([2]uint32)
		d.durationData = append(d.durationData, tmp[0], tmp[1])
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		d.intData = append(d.intData, t.raw().(int32))
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		d.longData = append(d.longData, t.raw().(int64))
	case DtInt128, DtIP, DtUUID:
		tmp := t.raw().([2]uint64)
		d.long2Data = append(d.long2Data, tmp[0], tmp[1])
	case DtDecimal32:
		tmp := t.raw().([2]int32)
		d.decimal32Data = append(d.decimal32Data, tmp[1])
	case DtDecimal64:
		tmp := t.raw().([2]int64)
		d.decimal64Data = append(d.decimal64Data, tmp[1])
	case DtComplex, DtPoint:
		tmp := t.raw().([2]float64)
		d.double2Data = append(d.double2Data, tmp[0], tmp[1])
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		d.stringData = append(d.stringData, t.raw().(string))
	case DtBlob:
		d.blobData = append(d.blobData, t.raw().([]byte))
	case DtAny:
		d.anyData = append(d.anyData, t.raw().(DataForm))
	}
	d.count++
	return d
}

func (d *dataTypeList) Len() int {
	return d.count
}

func (d *dataTypeList) DataType() DataTypeByte {
	return d.t
}

func (d *dataTypeList) Get(ind int) DataType {
	if ind >= d.count {
		return nil
	}

	t := &dataType{
		t:  d.t,
		bo: d.bo,
	}

	switch d.t {
	case DtVoid, DtBool, DtChar:
		t.data = d.charData[ind]
	case DtShort:
		t.data = d.shortData[ind]
	case DtFloat:
		t.data = d.floatData[ind]
	case DtDouble:
		t.data = d.doubleData[ind]
	case DtDuration:
		i := 2 * ind
		t.data = [2]uint32{d.durationData[i], d.durationData[i+1]}
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		t.data = d.intData[ind]
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		t.data = d.longData[ind]
	case DtInt128, DtIP, DtUUID:
		i := 2 * ind
		t.data = [2]uint64{d.long2Data[i], d.long2Data[i+1]}
	case DtDecimal32:
		t.data = [2]int32{d.decimal32Data[0], d.decimal32Data[ind+1]}
	case DtDecimal64:
		t.data = [2]int64{d.decimal64Data[0], d.decimal64Data[ind+1]}
	case DtComplex, DtPoint:
		i := 2 * ind
		t.data = [2]float64{d.double2Data[i], d.double2Data[i+1]}
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		t.data = d.stringData[ind]
	case DtBlob:
		t.data = d.blobData[ind]
	case DtAny:
		t.data = d.anyData[ind]
	}

	return t
}

func (d *dataTypeList) GetSubList(indexes []int) DataTypeList {
	length := len(indexes)
	res := &dataTypeList{
		t:     d.t,
		bo:    d.bo,
		count: length,
	}

	switch d.t {
	case DtVoid, DtBool, DtChar:
		res.charData = make([]uint8, length)
		for k, v := range indexes {
			res.charData[k] = d.charData[v]
		}
	case DtShort:
		res.shortData = make([]int16, length)
		for k, v := range indexes {
			res.shortData[k] = d.shortData[v]
		}
	case DtFloat:
		res.floatData = make([]float32, length)
		for k, v := range indexes {
			res.floatData[k] = d.floatData[v]
		}
	case DtDouble:
		res.doubleData = make([]float64, length)
		for k, v := range indexes {
			res.doubleData[k] = d.doubleData[v]
		}
	case DtDuration:
		res.durationData = make([]uint32, 0, 2*length)
		for _, v := range indexes {
			ind := 2 * v
			res.durationData = append(res.durationData, d.durationData[ind], d.durationData[ind+1])
		}
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		res.intData = make([]int32, length)
		for k, v := range indexes {
			res.intData[k] = d.intData[v]
		}
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		res.longData = make([]int64, length)
		for k, v := range indexes {
			res.longData[k] = d.longData[v]
		}
	case DtInt128, DtIP, DtUUID:
		res.long2Data = make([]uint64, 0, 2*length)
		for _, v := range indexes {
			ind := 2 * v
			res.long2Data = append(res.long2Data, d.long2Data[ind], d.long2Data[ind+1])
		}
	case DtDecimal32:
		res.decimal32Data = make([]int32, 0, length+1)
		res.decimal32Data = append(res.decimal32Data, d.decimal32Data[0])
		for _, v := range indexes {
			res.decimal32Data = append(res.decimal32Data, d.decimal32Data[v+1])
		}
	case DtDecimal64:
		res.decimal64Data = make([]int64, 0, 1+length)
		res.decimal64Data = append(res.decimal64Data, d.decimal64Data[0])
		for _, v := range indexes {
			res.decimal64Data = append(res.decimal64Data, d.decimal64Data[v+1])
		}
	case DtComplex, DtPoint:
		res.double2Data = make([]float64, 0, 2*length)
		for _, v := range indexes {
			ind := 2 * v
			res.double2Data = append(res.double2Data, d.double2Data[ind], d.double2Data[ind+1])
		}
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res.stringData = make([]string, length)
		for k, v := range indexes {
			res.stringData[k] = d.stringData[v]
		}
	case DtBlob:
		res.blobData = make([][]byte, length)
		for k, v := range indexes {
			res.blobData[k] = d.blobData[v]
		}
	case DtAny:
		res.anyData = make([]DataForm, length)
		for k, v := range indexes {
			res.anyData[k] = d.anyData[v]
		}
	}

	return res
}

func (d *dataTypeList) Sub(start, end int) DataTypeList {
	if start < 0 || d.Len() < end || start >= end {
		return nil
	}

	res := &dataTypeList{
		count: end - start,
		t:     d.t,
		bo:    d.bo,
	}

	switch d.t {
	case DtVoid, DtBool, DtChar:
		res.charData = d.charData[start:end]
	case DtShort:
		res.shortData = d.shortData[start:end]
	case DtFloat:
		res.floatData = d.floatData[start:end]
	case DtDouble:
		res.doubleData = d.doubleData[start:end]
	case DtDuration:
		res.durationData = d.durationData[2*start : 2*end]
	case DtInt, DtDate, DtMonth, DtTime, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		res.intData = d.intData[start:end]
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		res.longData = d.longData[start:end]
	case DtInt128, DtIP, DtUUID:
		res.long2Data = d.long2Data[2*start : 2*end]
	case DtDecimal32:
		res.decimal32Data = make([]int32, 0, res.count+1)
		res.decimal32Data = append(res.decimal32Data, d.decimal32Data[0])
		res.decimal32Data = append(res.decimal32Data, d.decimal32Data[start+1:end+1]...)
	case DtDecimal64:
		res.decimal64Data = make([]int64, 0, res.count+1)
		res.decimal64Data = append(res.decimal64Data, d.decimal64Data[0])
		res.decimal64Data = append(res.decimal64Data, d.decimal64Data[start+1:end+1]...)
	case DtComplex, DtPoint:
		res.double2Data = d.double2Data[2*start : 2*end]
	case DtString, DtCode, DtFunction, DtHandle, DtSymbol:
		res.stringData = d.stringData[start:end]
	case DtBlob:
		res.blobData = d.blobData[start:end]
	case DtAny:
		res.anyData = d.anyData[start:end]
	}

	return res
}

func (d *dataTypeList) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	if d.Len() == 0 {
		return nil
	}

	var err error
	switch d.t {
	case DtString, DtCode, DtFunction, DtHandle, DtDictionary, DtSymbol:
		err = writeStrings(w, d.stringData)
	case DtBlob:
		err = writeBlobs(w, bo, d.blobData)
	case DtAny:
		for _, v := range d.anyData {
			err := v.Render(w, bo)
			if err != nil {
				return err
			}
		}
	case DtBool, DtChar, DtCompress:
		err = w.Write(d.charData)
	case DtInt, DtTime, DtDate, DtMonth, DtMinute, DtSecond, DtDatetime, DtDateHour, DtDateMinute:
		err = w.Write(protocol.ByteSliceFromInt32Slice(d.intData))
	case DtShort:
		err = w.Write(protocol.ByteSliceFromInt16Slice(d.shortData))
	case DtVoid:
		err = writeVoids(w, d.count)
	case DtDouble:
		err = w.Write(protocol.ByteSliceFromFloat64Slice(d.doubleData))
	case DtFloat:
		err = w.Write(protocol.ByteSliceFromFloat32Slice(d.floatData))
	case DtLong, DtTimestamp, DtNanoTime, DtNanoTimestamp:
		err = w.Write(protocol.ByteSliceFromInt64Slice(d.longData))
	case DtDecimal32:
		err = w.Write(protocol.ByteSliceFromInt32Slice(d.decimal32Data))
	case DtDecimal64:
		err = writeDecimal64s(w, bo, d.decimal64Data)
	case DtDuration:
		err = w.Write(protocol.ByteSliceFromUint32Slice(d.durationData))
	case DtPoint, DtComplex:
		err = w.Write(protocol.ByteSliceFromFloat64Slice(d.double2Data))
	case DtInt128, DtUUID, DtIP:
		err = w.Write(protocol.ByteSliceFromUint64Slice(d.long2Data))
	}

	return err
}

func (d *dataTypeList) StringList() []string {
	switch d.t {
	case DtDecimal32:
		return decimal32sString(d.decimal32Data)
	case DtDecimal64:
		return decimal64sString(d.decimal64Data)
	}

	tmp := d.Value()
	res := make([]string, len(tmp))
	if d.t == DtDate || d.t == DtDateHour || d.t == DtDatetime || d.t == DtDateMinute || d.t == DtMinute || d.t == DtMonth ||
		d.t == DtNanoTime || d.t == DtNanoTimestamp || d.t == DtTime || d.t == DtTimestamp ||
		d.t == DtSecond {
		times := make([]time.Time, len(tmp))
		for k, v := range tmp {
			times[k] = v.(time.Time)
		}
		return formatTime(d.t, times)
	}

	switch {
	case d.t == DtBlob:
		for k, v := range tmp {
			res[k] = fmt.Sprintf("%s", v)
		}
	case d.t != DtUUID && d.t != DtIP && d.t != DtPoint && d.t != DtInt128:
		for k, v := range tmp {
			if d.IsNull(k) {
				res[k] = ""
			} else if d.t == DtFloat || d.t == DtDouble {
				res[k] = floatString(v)
			} else if d.t == DtInt || d.t == DtShort || d.t == DtLong {
				res[k] = fmt.Sprintf("%d", v)
			} else {
				res[k] = fmt.Sprintf("%v", v)
			}
		}
	default:
		for k, v := range tmp {
			res[k] = fmt.Sprintf("%v", v)
		}
	}

	return res
}

func floatString(val interface{}) string {
	switch val.(type) {
	case float32:
		return strconv.FormatFloat(float64(val.(float32)), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val.(float64), 'f', -1, 64)
	}
	return ""
}

func decimal32sString(d32 []int32) []string {
	res := make([]string, len(d32)-1)
	sca := d32[0]
	for i := 1; i < len(d32); i++ {
		val := d32[i]
		if val == NullInt {
			res[i-1] = ""
			continue
		}

		f := decimal.NewFromFloat(decimal32Value(sca, val).(float64))
		res[i-1] = f.StringFixed(sca)
	}

	return res
}

func decimal64sString(d64 []int64) []string {
	res := make([]string, len(d64)-1)
	sca := d64[0]
	for i := 1; i < len(d64); i++ {
		val := d64[i]
		if val == NullLong {
			res[i-1] = ""
			continue
		}

		f := decimal.NewFromFloat(decimal64Value(sca, val).(float64))
		res[i-1] = f.StringFixed(int32(sca))
	}

	return res
}

func formatTime(dt DataTypeByte, times []time.Time) []string {
	res := make([]string, len(times))
	layout := ""

	switch dt {
	case DtDate:
		layout = "2006.01.02"
	case DtDateHour:
		layout = "2006.01.02T15"
	case DtDatetime:
		layout = "2006.01.02T15:04:05"
	case DtDateMinute:
		layout = "2006.01.02T15:04"
	case DtMinute:
		layout = "15:04m"
	case DtMonth:
		layout = "2006.01M"
	case DtNanoTime:
		layout = "15:04:05.000000000"
	case DtNanoTimestamp:
		layout = "2006.01.02T15:04:05.000000000"
	case DtSecond:
		layout = "15:04:05"
	case DtTime:
		layout = "15:04:05.000"
	case DtTimestamp:
		layout = "2006.01.02T15:04:05.000"
	}

	for k, v := range times {
		if v != emptyTime {
			res[k] = v.Format(layout)
		} else {
			res[k] = ""
		}
	}

	return res
}

func (d *dataTypeList) shortAsOf(t DataType) int {
	s2 := t.Value().(int16)
	val := d.shortData
	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if s1 <= s2 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) intAsOf(t DataType) int {
	s2 := t.raw().(int32)
	val := d.intData
	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if s1 <= s2 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) longAsOf(t DataType) int {
	s2 := t.raw().(int64)
	val := d.longData
	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if s1 <= s2 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) charAsOf(t DataType) int {
	s2 := t.raw().(uint8)
	val := d.charData

	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if s1 <= s2 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) doubleAsOf(t DataType) int {
	s2 := t.raw().(float64)
	val := d.doubleData

	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if s1 <= s2 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) floatAsOf(t DataType) int {
	s2 := t.raw().(float32)
	val := d.floatData
	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if s1 <= s2 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) stringAsOf(t DataType) int {
	s2 := t.raw().(string)
	val := d.stringData
	end := d.Len() - 1
	st := 0
	for st <= end {
		mid := (st + end) / 2

		s1 := val[mid]
		if strings.Compare(s1, s2) <= 0 {
			st = mid + 1
		} else {
			end = mid - 1
		}
	}

	return end
}

func (d *dataTypeList) renderDuration(val interface{}) error {
	str, ok := val.([]string)
	if !ok {
		return errors.New("the type of input must be []string when datatype is DtDuration")
	}

	length := len(str)
	d.count = length
	d.durationData = make([]uint32, 0, 2*length)
	for _, v := range str {
		if v == "" {
			d.durationData = append(d.durationData, emptyDuration[0], emptyDuration[1])
		} else {
			tmp, err := renderDurationFromString(v)
			if err != nil {
				return err
			}

			d.durationData = append(d.durationData, tmp[0], tmp[1])
		}
	}

	return nil
}

func (d *dataTypeList) renderDouble2(val interface{}) error {
	f64s, ok := val.([][2]float64)
	if !ok {
		return errors.New("the type of input must be [][2]float64 when datatype is DtComplex or DtPoint")
	}

	length := len(f64s)
	d.count = length
	d.double2Data = make([]float64, 0, 2*length)
	for _, v := range f64s {
		d.double2Data = append(d.double2Data, v[0], v[1])
	}

	return nil
}

func (d *dataTypeList) renderBool(val interface{}) error {
	var bs []byte
	switch v := val.(type) {
	case []byte:
		bs = v
	case []bool:
		bl := v
		bs = make([]byte, len(bl))
		for k, v := range bl {
			bs[k] = boolToByte(v)
		}
	default:
		return errors.New("the type of input must be []byte or []bool when datatype is DtBool")
	}

	length := len(bs)
	d.count = length
	d.charData = make([]uint8, length)
	for k, v := range bs {
		d.charData[k] = renderBoolFromByte(v)
	}
	return nil
}

func (d *dataTypeList) renderBlob(val interface{}) error {
	byt, ok := val.([][]byte)
	if !ok {
		return errors.New("the type of input must be [][]byte when datatype is DtBlob")
	}

	d.count = len(byt)
	d.blobData = byt
	return nil
}

func (d *dataTypeList) renderByte(val interface{}) error {
	bs, ok := val.([]byte)
	if !ok {
		return errors.New("the type of input must be []byte when datatype is DtChar or DtCompress")
	}

	length := len(bs)
	d.count = length
	d.charData = bs

	return nil
}

func (d *dataTypeList) renderDate(val interface{}) error {
	ts, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtDate")
	}

	length := len(ts)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range ts {
		d.intData[k] = renderDateFromTime(v)
	}
	return nil
}

func (d *dataTypeList) renderDateMinute(val interface{}) error {
	ts, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtDateMinute")
	}

	length := len(ts)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range ts {
		d.intData[k] = renderDateMinuteFromTime(v)
	}
	return nil
}

func (d *dataTypeList) renderDateHour(val interface{}) error {
	ts, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtDateHour")
	}

	length := len(ts)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range ts {
		d.intData[k] = renderDateHourFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderDateTime(val interface{}) error {
	ts, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtDatetime")
	}

	length := len(ts)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range ts {
		d.intData[k] = renderDateTimeFromTime(v)
	}
	return nil
}

func (d *dataTypeList) renderDouble(val interface{}) error {
	f64s, ok := val.([]float64)
	if !ok {
		return errors.New("the type of input must be []float64 when datatype is DtDouble")
	}

	d.count = len(f64s)
	d.doubleData = f64s
	return nil
}

func (d *dataTypeList) renderFloat(val interface{}) error {
	f32s, ok := val.([]float32)
	if !ok {
		return errors.New("the type of input must be []float32 when datatype is DtFloat")
	}

	d.count = len(f32s)
	d.floatData = f32s
	return nil
}

func (d *dataTypeList) renderInt(val interface{}) error {
	i32s, ok := val.([]int32)
	if !ok {
		return errors.New("the type of input must be []int32 when datatype is DtInt")
	}

	d.count = len(i32s)
	d.intData = i32s
	return nil
}

func (d *dataTypeList) renderInt128(val interface{}) error {
	str, ok := val.([]string)
	if !ok {
		return errors.New("the type of input must be []string when datatype is DtInt128")
	}

	length := len(str)
	d.count = length
	d.long2Data = make([]uint64, 0, 2*length)
	for _, v := range str {
		tmp := renderInt128FromString(v)
		d.long2Data = append(d.long2Data, tmp[0], tmp[1])
	}

	return nil
}

func (d *dataTypeList) renderIP(val interface{}, bo protocol.ByteOrder) error {
	str, ok := val.([]string)
	if !ok {
		return errors.New("the type of input must be []string when datatype is DtIP")
	}

	length := len(str)
	d.count = length
	d.long2Data = make([]uint64, 0, 2*length)
	for _, v := range str {
		tmp := renderIPFromString(v, bo)
		d.long2Data = append(d.long2Data, tmp[0], tmp[1])
	}

	return nil
}

func (d *dataTypeList) renderDecimal32(val interface{}) error {
	dec, ok := val.(*Decimal32s)
	if !ok {
		return errors.New("the type of input must be *Decimal32s when datatype is DtDecimal32")
	}

	if dec.Scale < 0 || dec.Scale > 9 {
		return fmt.Errorf("Scale out of bound(valid range: [0, 9], but get: %d)", dec.Scale)
	}

	length := len(dec.Value)
	d.count = length
	d.decimal32Data = make([]int32, 0, length+1)
	d.decimal32Data = append(d.decimal32Data, dec.Scale)
	for _, v := range dec.Value {
		f, err := calculateDecimal32(dec.Scale, v)
		if err != nil {
			return err
		}
		d.decimal32Data = append(d.decimal32Data, int32(f))
	}

	return nil
}

func (d *dataTypeList) renderDecimal64(val interface{}) error {
	dec, ok := val.(*Decimal64s)
	if !ok {
		return errors.New("the type of input must be *Decimal64s when datatype is DtDecimal64")
	}

	if dec.Scale < 0 || dec.Scale > 18 {
		return fmt.Errorf("Scale out of bound(valid range: [0, 18], but get: %d)", dec.Scale)
	}

	length := len(dec.Value)
	d.count = length
	d.decimal64Data = make([]int64, 0, length+1)
	d.decimal64Data = append(d.decimal64Data, int64(dec.Scale))
	for _, v := range dec.Value {
		f, err := calculateDecimal64(dec.Scale, v)
		if err != nil {
			return err
		}
		d.decimal64Data = append(d.decimal64Data, int64(f))
	}

	return nil
}

func (d *dataTypeList) renderLong(val interface{}) error {
	is, ok := val.([]int64)
	if !ok {
		return errors.New("the type of input must be []int64 when datatype is DtLong")
	}

	d.count = len(is)
	d.longData = is

	return nil
}

func (d *dataTypeList) renderMinute(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtMinute")
	}

	length := len(tis)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range tis {
		d.intData[k] = renderMinuteFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderMonth(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtMonth")
	}

	length := len(tis)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range tis {
		d.intData[k] = renderMonthFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderNanoTime(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtNanoTime")
	}

	length := len(tis)
	d.count = length
	d.longData = make([]int64, length)
	for k, v := range tis {
		d.longData[k] = renderNanoTimeFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderNanoTimestamp(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtNanoTimestamp")
	}

	length := len(tis)
	d.count = length
	d.longData = make([]int64, length)
	for k, v := range tis {
		d.longData[k] = renderNanoTimestampFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderSecond(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtSecond")
	}

	length := len(tis)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range tis {
		d.intData[k] = renderSecondFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderShort(val interface{}) error {
	is, ok := val.([]int16)
	if !ok {
		return errors.New("the type of input must be []int16 when datatype is DtShort")
	}

	length := len(is)
	d.count = length
	d.shortData = is

	return nil
}

func (d *dataTypeList) renderTime(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtTime")
	}

	length := len(tis)
	d.count = length
	d.intData = make([]int32, length)
	for k, v := range tis {
		d.intData[k] = renderTimeFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderTimestamp(val interface{}) error {
	tis, ok := val.([]time.Time)
	if !ok {
		return errors.New("the type of input must be []time.Time when datatype is DtTimestamp")
	}

	length := len(tis)
	d.count = length
	d.longData = make([]int64, length)
	for k, v := range tis {
		d.longData[k] = renderTimestampFromTime(v)
	}

	return nil
}

func (d *dataTypeList) renderUUID(val interface{}) error {
	str, ok := val.([]string)
	if !ok {
		return errors.New("the type of input must be []string when datatype is DtUuid")
	}

	length := len(str)
	d.count = length
	d.long2Data = make([]uint64, 0, 2*length)
	for _, v := range str {
		tmp := renderUUIDFromString(v)
		d.long2Data = append(d.long2Data, tmp[0], tmp[1])
	}

	return nil
}

func (d *dataTypeList) renderAny(val interface{}) error {
	dataForms, ok := val.([]DataForm)
	if !ok {
		return errors.New("the type of input must be []DataForm when datatype is DtAny")
	}

	d.count = len(dataForms)
	d.anyData = dataForms

	return nil
}

func (d *dataTypeList) renderString(val interface{}) error {
	str, ok := val.([]string)
	if !ok {
		return errors.New("the type of input must be []string when datatype is DtString, DtCode, DtFunction, DtHandle or DtSymbol")
	}

	d.count = len(str)
	d.stringData = str

	return nil
}

func parseStrings(raw []string, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseUUIDs(count int, raw []uint64, res []interface{}, bo protocol.ByteOrder) {
	for i := 0; i < count; i++ {
		ind := 2 * i
		high, low := make([]byte, protocol.Uint64Size), make([]byte, protocol.Uint64Size)
		bo.PutUint64(high, raw[ind+1])
		bo.PutUint64(low, raw[ind])

		res[i] = fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", bo.Uint32(high[4:]), bo.Uint16(high[2:4]),
			bo.Uint16(high[0:2]), bo.Uint16(low[6:8]), bo.Uint64(append(low[0:6], 0, 0)))
	}
}

func parseTimeStamps(raw []int64, res []interface{}) {
	for k, v := range raw {
		res[k] = parseTimeStamp(v)
	}
}

func parseTimes(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseTime(v)
	}
}

func parseShorts(raw []int16, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseComplexes(count int, raw []float64, res []interface{}) {
	for i := 0; i < count; i++ {
		ind := 2 * i
		if raw[ind] == -math.MaxFloat64 || raw[ind+1] == -math.MaxFloat64 {
			res[i] = ""
			continue
		}

		res[i] = fmt.Sprintf("%.5f+%.5fi", raw[ind], raw[ind+1])
	}
}

func parsePoints(count int, raw []float64, res []interface{}) {
	for i := 0; i < count; i++ {
		ind := 2 * i
		if raw[ind] == -math.MaxFloat64 || raw[ind+1] == -math.MaxFloat64 {
			res[i] = emptyPoint
			continue
		}

		res[i] = fmt.Sprintf("(%.5f, %.5f)", raw[ind], raw[ind+1])
	}
}

func parseNanoTimeStamps(raw []int64, res []interface{}) {
	for k, v := range raw {
		res[k] = parseNanoTimeStamp(v)
	}
}

func parseNanoTimes(raw []int64, res []interface{}) {
	for k, v := range raw {
		res[k] = parseNanoTime(v)
	}
}

func parseMonths(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseMonth(v)
	}
}

func parseMinutes(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseMinute(v)
	}
}

func parseSeconds(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseSecond(v)
	}
}

func parseLongs(raw []int64, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseIPs(count int, raw []uint64, res []interface{}, bo protocol.ByteOrder) {
	for i := 0; i < count; i++ {
		ind := 2 * i
		if raw[ind] == 0 && raw[ind+1] == 0 {
			res[i] = NullIP
			continue
		}

		low := make([]byte, protocol.Uint64Size)
		bo.PutUint64(low, raw[ind])
		if raw[ind+1] == 0 {
			res[i] = fmt.Sprintf("%d.%d.%d.%d", low[3], low[2], low[1], low[0])
			continue
		}

		high := make([]byte, protocol.Uint64Size)
		bo.PutUint64(high, raw[ind+1])
		res[i] = fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x", bo.Uint16(high[6:8]), bo.Uint16(high[4:6]), bo.Uint16(high[2:4]),
			bo.Uint16(high[0:2]), bo.Uint16(low[6:8]), bo.Uint16(low[4:6]), bo.Uint16(low[2:4]), bo.Uint16(low[0:2]))
	}
}

func parseDecimal32s(count int, raw []int32, res []interface{}) {
	scale := raw[0]
	for i := 0; i < count; i++ {
		res[i] = decimal32(scale, raw[i+1])
	}
}

func decimal64(scale, value int64) *Decimal64 {
	if value == NullLong {
		return &Decimal64{Scale: int32(scale), Value: float64(value)}
	}
	return &Decimal64{Scale: int32(scale), Value: float64(value) / math.Pow10(int(scale))}
}

func decimal32(scale, value int32) *Decimal32 {
	if value == NullInt {
		return &Decimal32{Scale: int32(scale), Value: float64(value)}
	}
	return &Decimal32{Scale: int32(scale), Value: float64(value) / math.Pow10(int(scale))}
}

func decimal64Value(scale, value int64) interface{} {
	switch {
	case value == NullLong:
		return NullLong
	case scale == 0:
		return float64(value)
	default:
		return float64(value) / math.Pow10(int(scale))
	}
}

func decimal32Value(scale, value int32) interface{} {
	switch {
	case value == NullInt:
		return NullInt
	case scale == 0:
		return float64(value)
	default:
		return float64(value) / math.Pow10(int(scale))
	}
}

func parseDecimal64s(count int, raw []int64, res []interface{}) {
	scale := raw[0]
	for i := 0; i < count; i++ {
		res[i] = decimal64(scale, raw[i+1])
	}
}

func parseAny(raw []DataForm, res []interface{}) {
	for k, v := range raw {
		if v == nil {
			res[k] = nil
			continue
		}

		res[k] = v
	}
}

func parseInt128s(count int, raw []uint64, res []interface{}) {
	for i := 0; i < count; i++ {
		ind := 2 * i
		res[i] = generateInt128String(raw[ind+1], raw[ind])
	}
}

func generateInt128String(high, low uint64) string {
	var tmp string
	if high == 0 {
		tmp = "0000000000000000"
	} else {
		tmp = fmt.Sprintf("%16x", high)
	}
	if low == 0 {
		tmp += "0000000000000000"
	} else {
		tmp += fmt.Sprintf("%16x", low)
	}

	return tmp
}

func parseInt(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseDurations(count int, raw []uint32, res []interface{}) {
	for i := 0; i < count; i++ {
		ind := 2 * i
		if raw[ind] == MinInt32 {
			res[i] = ""
			continue
		}

		unit := durationUnit[raw[ind+1]]
		res[i] = fmt.Sprintf("%d%s", raw[ind], unit)
	}
}

func parseFloats(raw []float32, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseDoubles(raw []float64, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseDateTimes(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseDateTime(v)
	}
}

func parseDateMinutes(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseDateMinute(v)
	}
}

func parseDateHours(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseDateHour(v)
	}
}

func parseDates(raw []int32, res []interface{}) {
	for k, v := range raw {
		res[k] = parseDate(v)
	}
}

func parseBytes(raw []byte, res []interface{}) {
	for k, v := range raw {
		res[k] = int8(v)
	}
}

func parseBlobs(raw [][]byte, res []interface{}) {
	for k, v := range raw {
		res[k] = v
	}
}

func parseBools(raw []uint8, res []interface{}) {
	for k, v := range raw {
		if v == MinInt8 {
			res[k] = int8(math.MinInt8)
		} else {
			res[k] = v == 1
		}
	}
}

func (d *Decimal64s) String() string {
	res := make([]string, len(d.Value))
	for k, v := range d.Value {
		f, err := calculateDecimal64(d.Scale, v)
		if f != NullDecimal64Value && err == nil {
			res[k] = decimal.NewFromFloat(v).StringFixed(d.Scale)
		}
	}

	return fmt.Sprintf("[%s]", strings.Join(res, ","))
}

func (d *Decimal32s) String() string {
	res := make([]string, len(d.Value))
	for k, v := range d.Value {
		f, err := calculateDecimal32(d.Scale, v)
		if f != NullDecimal32Value && err == nil {
			res[k] = decimal.NewFromFloat(v).StringFixed(d.Scale)
		}
	}

	return fmt.Sprintf("[%s]", strings.Join(res, ", "))
}
