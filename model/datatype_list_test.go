package model

import (
	"bytes"
	"testing"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"

	"github.com/stretchr/testify/assert"
)

func TestDataTypeList(t *testing.T) {
	dt, err := NewDataType(DtInt, int32(10))
	assert.Nil(t, err)

	dtl := NewDataTypeList(DtInt, []DataType{dt})
	assert.Equal(t, dtl.DataType(), DtInt)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.AsOf(dt), 0)

	sl := dtl.StringList()
	assert.Equal(t, sl, []string{"10"})

	d := dtl.Get(0)
	assert.Equal(t, d.DataType(), DtInt)

	dtl = dtl.Append(d)
	assert.Equal(t, dtl.Len(), 2)

	sl = dtl.StringList()
	assert.Equal(t, sl, []string{"10", "10"})

	dtl = dtl.Sub(0, 1)
	assert.Equal(t, dtl.Len(), 1)

	sl = dtl.StringList()
	assert.Equal(t, sl, []string{"10"})
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dt1, err := NewDataType(DtInt, int32(20))
	assert.Nil(t, err)

	err = dtl.Set(0, dt1)
	assert.Nil(t, err)

	sl = dtl.StringList()
	assert.Equal(t, sl, []string{"20"})

	err = dtl.Set(1, dt1)
	assert.Equal(t, err.Error(), "index 1 exceeds the number of data 1")

	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)
	err = dtl.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x14\x00\x00\x00")

	vct := NewVector(dtl)
	dt, err = NewDataType(DtAny, vct)
	assert.Nil(t, err)

	dtl = NewDataTypeList(DtAny, []DataType{dt})

	sl = dtl.StringList()
	assert.Equal(t, sl, []string{"vector<int>([20])"})
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl = NewEmptyDataTypeList(DtAny, 1)
	sl = dtl.StringList()
	assert.Equal(t, sl, []string{""})

	dtl = NewEmptyDataTypeList(DtString, 1)
	sl = dtl.StringList()
	assert.Equal(t, sl, []string{""})

	dt, err = NewDataType(DtString, "10")
	assert.Nil(t, err)
	err = dtl.Set(0, dt)
	assert.Nil(t, err)

	str := dtl.ElementString(0)
	assert.Equal(t, str, "10")

	dt1, err = NewDataType(DtString, "20")
	assert.Nil(t, err)

	dtl.Append(dt1)
	assert.Equal(t, dtl.AsOf(dt), 0)
	assert.Equal(t, dtl.AsOf(dt1), 1)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "10")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtChar, []byte{0, 1})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 2)
	assert.Equal(t, dtl.DataType(), DtChar)

	dt, err = NewDataType(DtChar, byte(1))
	assert.Nil(t, err)
	assert.Equal(t, dtl.AsOf(dt), 1)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "0")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtInt, []int32{1, 2, 3, 4})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 4)

	sl = dtl.StringList()
	assert.Equal(t, sl, []string{"1", "2", "3", "4"})

	dtl = dtl.GetSubList([]int{1, 3})

	sl = dtl.StringList()
	assert.Equal(t, sl, []string{"2", "4"})

	dtl, err = NewDataTypeListWithRaw(DtBool, []byte{1})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtBool)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "true")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtBlob, [][]byte{{0, 1, 1}})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtBlob)

	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0x3, 0x0, 0x0, 0x0, 0x0, 0x1, 0x1})

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "\x00\x01\x01")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dt, err = NewDataType(DtVoid, nil)
	assert.Nil(t, err)

	dtl = NewDataTypeList(DtVoid, []DataType{dt})
	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0x0})

	dtl, err = NewDataTypeListWithRaw(DtComplex, [][2]float64{{1, 1}})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtComplex)

	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "1.00000+1.00000i")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtDouble+64, []float64{1})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtDouble)

	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf0, 0x3f})

	dt, err = NewDataType(DtDouble, float64(1))
	assert.Nil(t, err)
	assert.Equal(t, dtl.AsOf(dt), 0)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "1")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtTime, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtTime)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "02:02:02.000")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtDateHour, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtDateHour)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "2022.05.01T02")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtDate, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtDate)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "2022.05.01")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtDatetime, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtDatetime)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "2022.05.01T02:02:02")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtMinute, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtMinute)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "02:02m")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtMonth, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtMonth)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "2022.05M")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtNanoTime, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtNanoTime)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "02:02:02.000000020")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtSecond, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtSecond)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "02:02:02")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtTimestamp, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtTimestamp)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "2022.05.01T02:02:02.000")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtNanoTimestamp, []time.Time{time.Date(2022, 5, 1, 2, 2, 2, 20, time.UTC)})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtNanoTimestamp)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "2022.05.01T02:02:02.000000020")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtUUID, []string{"e5eca940-5b99-45d0-bf1c-620f6b1b9d5b"})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtUUID)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "e5eca940-5b99-45d0-bf1c-620f6b1b9d5b")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "00000000-0000-0000-0000-000000000000")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33"})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 2)
	assert.Equal(t, dtl.DataType(), DtInt128)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "e1671797c52e15f763380b45e841ec32")
	assert.Equal(t, sl[1], "e1671797c52e15f763380b45e841ec33")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "00000000000000000000000000000000")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtPoint, [][2]float64{{1, 1}})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtPoint)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "(1.00000, 1.00000)")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], emptyPoint)
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtDuration, []string{"10H"})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtDuration)

	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0xa, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0x0})

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "10H")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtIP, []string{"346b:6c2a:3347:d244:7654:5d5a:bcbb:5dc7"})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 1)
	assert.Equal(t, dtl.DataType(), DtIP)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "346b:6c2a:3347:d244:7654:5d5a:bcbb:5dc7")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "0.0.0.0")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtIP, []string{"127.0.0.1", "127.0.0.2"})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 2)
	assert.Equal(t, dtl.DataType(), DtIP)

	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0x1, 0x0, 0x0, 0x7f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x7f, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0})

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "127.0.0.1")
	assert.Equal(t, sl[1], "127.0.0.2")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "0.0.0.0")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtFloat, []float32{1, 2})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 2)
	assert.Equal(t, dtl.DataType(), DtFloat)

	by.Reset()
	err = dtl.Render(w, protocol.LittleEndian)
	assert.Nil(t, err)

	w.Flush()
	assert.Equal(t, by.Bytes(), []byte{0x0, 0x0, 0x80, 0x3f, 0x0, 0x0, 0x0, 0x40})

	dt, err = NewDataType(DtFloat, float32(2.0))
	assert.Nil(t, err)
	assert.Equal(t, dtl.AsOf(dt), 1)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "1")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtLong, []int64{1, 2})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 2)
	assert.Equal(t, dtl.DataType(), DtLong)

	dt, err = NewDataType(DtLong, int64(2))
	assert.Nil(t, err)
	assert.Equal(t, dtl.AsOf(dt), 1)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "1")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	dtl, err = NewDataTypeListWithRaw(DtShort, []int16{1, 2})
	assert.Nil(t, err)
	assert.Equal(t, dtl.Len(), 2)
	assert.Equal(t, dtl.DataType(), DtShort)

	dt, err = NewDataType(DtShort, int16(2.0))
	assert.Nil(t, err)
	assert.Equal(t, dtl.AsOf(dt), 1)

	sl = dtl.StringList()
	assert.Equal(t, sl[0], "1")
	assert.False(t, dtl.IsNull(0))

	dtl.SetNull(0)
	sl = dtl.StringList()
	assert.Equal(t, sl[0], "")
	assert.True(t, dtl.IsNull(0))

	_, err = NewDataTypeListWithRaw(DtLong, []int32{1, 2})
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "the type of input must be []int64 when datatype is DtLong")
}

func TestNewDataTypeListWithRawWithNullValue(t *testing.T) {
	dt, err := NewDataTypeListWithRaw(DtBool, []byte{1, NullBool})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtChar, []byte{97, NullChar})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtShort, []int16{1, NullShort})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtLong, []int64{1, NullLong})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtDate, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtMonth, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtTime, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtMinute, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtSecond, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtDatetime, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtTimestamp, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtNanoTime, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtNanoTimestamp, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtFloat, []float32{1.0, NullFloat})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtDouble, []float64{1.0, NullDouble})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtSymbol, []string{"sym", NullString})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtString, []string{"str", NullString})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtUUID, []string{"e5eca940-5b99-45d0-bf1c-620f6b1b9d5b", NullUUID})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "00000000-0000-0000-0000-000000000000")

	dt, err = NewDataTypeListWithRaw(DtAny, []DataForm{nil, NullAny})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtCompress, []byte{0, NullCompress})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtDateHour, []time.Time{originalTime, NullTime})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtIP, []string{"127.0.0.1", NullIP})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "0.0.0.0")

	dt, err = NewDataTypeListWithRaw(DtInt128, []string{"e1671797c52e15f763380b45e841ec32", NullInt128})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "00000000000000000000000000000000")

	dt, err = NewDataTypeListWithRaw(DtBlob, [][]byte{{0, 1}, NullBlob})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtComplex, [][2]float64{{1, 1}, NullComplex})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")

	dt, err = NewDataTypeListWithRaw(DtPoint, [][2]float64{{1, 1}, NullPoint})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), emptyPoint)

	dt, err = NewDataTypeListWithRaw(DtDuration, []string{"10m", NullDuration})
	assert.Nil(t, err)
	assert.True(t, dt.IsNull(1))
	assert.Equal(t, dt.ElementString(1), "")
}
