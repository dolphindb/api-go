package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDatatype(t *testing.T) {
	dt, err := NewDataType(DtVoid, nil)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtVoid)

	str := dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtBool, byte(1))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtBool)

	str = dt.String()
	assert.Equal(t, str, "true")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtBlob, []byte{1, 2, 3, 4, 5})
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtBlob)
	assert.Equal(t, dt.Value(), []byte{1, 2, 3, 4, 5})

	str = dt.String()
	assert.Equal(t, str, "\x01\x02\x03\x04\x05")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtChar, byte(97))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtChar)

	str = dt.String()
	assert.Equal(t, str, "97")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	buckets := []int{13, 43, 71, 97, 4097}
	v, b := -127, -128
	dl, _ := NewDataTypeListFromRawData(DtChar, []uint8{127, uint8(v), 12, 0, uint8(b)})
	expectCharHashBuckets := []int{10, 12, 12, 0, -1, 41, 18, 12, 0, -1, 56, 24, 12, 0, -1, 30, 5, 12, 0, -1, 127, 129, 12, 0, -1}
	count := 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			assert.Equal(t, dl.Get(j).HashBucket(buckets[i]), expectCharHashBuckets[count])
			count++
		}
	}

	dt, err = NewDataType(DtComplex, [2]float64{1, 1})
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtComplex)

	str = dt.String()
	assert.Equal(t, str, "1.00000+1.00000i")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtDecimal32, &Decimal32{4, 0.1})
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDecimal32)

	str = dt.String()
	assert.Equal(t, str, "0.1000")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtDecimal64, &Decimal64{12, 0.1})
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDecimal64)

	str = dt.String()
	assert.Equal(t, str, "0.100000000000")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	ti := time.Date(1968, 11, 1, 23, 59, 59, 154140487, time.UTC)
	dt, err = NewDataType(DtDate, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDate)

	str = dt.String()
	assert.Equal(t, str, "1968.11.01")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	ti = time.Date(1968, 11, 1, 23, 59, 59, 154140487, time.UTC)
	dt, err = NewDataType(DtDateHour, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDateHour)

	str = dt.String()
	assert.Equal(t, str, "1968.11.01T23")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	ti = time.Date(1968, 11, 1, 23, 59, 59, 154140487, time.UTC)
	dt, err = NewDataType(DtDatetime, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDatetime)

	str = dt.String()
	assert.Equal(t, str, "1968.11.01T23:59:59")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtDouble+64, float64(1))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDouble)

	str = dt.String()
	assert.Equal(t, str, "1")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtFloat, float32(1.0))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtFloat)

	str = dt.String()
	assert.Equal(t, str, "1")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtDuration, "10H")
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtDuration)

	str = dt.String()
	assert.Equal(t, str, "10H")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtInt, int32(10))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtInt)

	str = dt.String()
	assert.Equal(t, str, "10")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dl, _ = NewDataTypeListFromRawData(DtInt, []int32{2147483647, -2147483647, 99, 0, -12})
	expectIntHashBuckets := []int{10, 12, 8, 0, 10, 7, 9, 13, 0, 4, 39, 41, 28, 0, 68, 65, 67, 2, 0, 23, 127, 129, 99, 0, 244}
	count = 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			assert.Equal(t, dl.Get(j).HashBucket(buckets[i]), expectIntHashBuckets[count])
			count++
		}
	}

	dt, err = NewDataType(DtInt128, "e1671797c52e15f763380b45e841ec32")
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtInt128)

	str = dt.String()
	assert.Equal(t, str, "e1671797c52e15f763380b45e841ec32")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "00000000000000000000000000000000")

	dl, _ = NewDataTypeListFromRawData(DtInt128, []string{"4b7545dc735379254fbf804dec34977f", "6f29ffbf80722c9fd386c6e48ca96340", "dd92685907f08a99ec5f8235c15a1588",
		"4f5387611b41d1385e272e6e866f862d", "130d6d5a0536c99ac7f9a01363b107c0"})
	expectInt128HashBuckets := []int{11, 6, 2, 3, 6, 42, 6, 30, 10, 32, 7, 47, 48, 31, 44, 15, 45, 75, 49, 44, 1116, 3479, 4032, 2053, 3150}
	count = 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			assert.Equal(t, dl.Get(j).HashBucket(buckets[i]), expectInt128HashBuckets[count])
			count++
		}
	}

	dt, err = NewDataType(DtIP, "346b:6c2a:3347:d244:7654:5d5a:bcbb:5dc7")
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtIP)

	str = dt.String()
	assert.Equal(t, str, "346b:6c2a:3347:d244:7654:5d5a:bcbb:5dc7")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "0.0.0.0")

	dt, err = NewDataType(DtLong, int64(1))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtLong)

	str = dt.String()
	assert.Equal(t, str, "1")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dl, _ = NewDataTypeListFromRawData(DtLong, []int64{9223372036854775807, -9223372036854775807, 12, 0, -12})
	expectLongHashBuckets := []int{7, 9, 12, 0, 4, 41, 0, 12, 0, 29, 4, 6, 12, 0, 69, 78, 80, 12, 0, 49, 4088, 4090, 12, 0, 4069}
	count = 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			assert.Equal(t, dl.Get(j).HashBucket(buckets[i]), expectLongHashBuckets[count])
			count++
		}
	}

	ti = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(100 * time.Hour)
	dt, err = NewDataType(DtMinute, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtMinute)

	str = dt.String()
	assert.Equal(t, str, "04:00m")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtMonth, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtMonth)

	str = dt.String()
	assert.Equal(t, str, "1970.01M")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	ti = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(100 * time.Hour)
	dt, err = NewDataType(DtNanoTime, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtNanoTime)

	str = dt.String()
	assert.Equal(t, str, "04:00:00.000000000")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	ti = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(100 * time.Hour)
	dt, err = NewDataType(DtNanoTimestamp, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtNanoTimestamp)

	str = dt.String()
	assert.Equal(t, str, "1970.01.05T04:00:00.000000000")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtPoint, [2]float64{1, 1})
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtPoint)

	str = dt.String()
	assert.Equal(t, str, "(1.00000, 1.00000)")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, emptyPoint)

	ti = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(100 * time.Hour)
	dt, err = NewDataType(DtSecond, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtSecond)

	str = dt.String()
	assert.Equal(t, str, "04:00:00")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtShort, int16(10))
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtShort)

	str = dt.String()
	assert.Equal(t, str, "10")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dl, _ = NewDataTypeListFromRawData(DtShort, []int16{32767, -32767, 12, 0, -12})
	expectShortHashBuckets := []int{7, 2, 12, 0, 10, 1, 15, 12, 0, 4, 36, 44, 12, 0, 68, 78, 54, 12, 0, 23, 4088, 265, 12, 0, 244}
	count = 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 5; j++ {
			assert.Equal(t, dl.Get(j).HashBucket(buckets[i]), expectShortHashBuckets[count])
			count++
		}
	}

	ti = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(100 * time.Hour)
	dt, err = NewDataType(DtTime, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtTime)

	str = dt.String()
	assert.Equal(t, str, "04:00:00.000")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	ti = time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(100 * time.Hour)
	dt, err = NewDataType(DtTimestamp, ti)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtTimestamp)

	str = dt.String()
	assert.Equal(t, str, "1970.01.05T04:00:00.000")

	s := NewScalar(dt)
	dt, err = NewDataType(DtAny, s)
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtAny)

	str = dt.String()
	assert.Equal(t, str, "timestamp(1970.01.05T04:00:00.000)")
	assert.Equal(t, str, dt.String())

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dt, err = NewDataType(DtSymbol, "datatype")
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtSymbol)

	str = dt.String()
	assert.Equal(t, str, "datatype")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "")

	dl, _ = NewDataTypeListFromRawData(DtString, []string{"!@#$%^&*()", "我是中文测试内容", "我是!@#$%^中文&*()", "e1281ls.zxl.d.,cxnv./';'sla", "abckdlskdful", ""})
	expectStringHashBuckets := []int{8, 11, 9, 12, 1, 0, 25, 3, 40, 28, 18, 0, 31, 14, 49, 8, 48, 0, 52, 92, 54, 4, 47, 0, 3892, 1574, 148, 3118, 1732, 0}
	count = 0
	for i := 0; i < 5; i++ {
		for j := 0; j < 6; j++ {
			assert.Equal(t, dl.Get(j).HashBucket(buckets[i]), expectStringHashBuckets[count])
			count++
		}
	}

	dt, err = NewDataType(DtUUID, "e5eca940-5b99-45d0-bf1c-620f6b1b9d5b")
	assert.Nil(t, err)
	assert.Equal(t, dt.DataType(), DtUUID)

	str = dt.String()
	assert.Equal(t, str, "e5eca940-5b99-45d0-bf1c-620f6b1b9d5b")

	dt.SetNull()
	str = dt.String()
	assert.Equal(t, str, "00000000-0000-0000-0000-000000000000")
}

func TestNewDataTypeWithNullValue(t *testing.T) {
	dt, err := NewDataType(DtBool, NullBool)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtChar, NullChar)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtShort, NullShort)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtLong, NullLong)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtDate, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtMonth, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtTime, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtMinute, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtSecond, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtDatetime, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtTimestamp, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtNanoTime, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtNanoTimestamp, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtFloat, NullFloat)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtDouble, NullDouble)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtSymbol, NullString)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtString, NullString)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtUUID, NullUUID)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "00000000-0000-0000-0000-000000000000")

	dt, err = NewDataType(DtAny, NullAny)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtCompress, NullCompress)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtDateHour, NullTime)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtIP, NullIP)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "0.0.0.0")

	dt, err = NewDataType(DtInt128, NullInt128)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "00000000000000000000000000000000")

	dt, err = NewDataType(DtBlob, NullBlob)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtComplex, NullComplex)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")

	dt, err = NewDataType(DtPoint, NullPoint)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), emptyPoint)

	dt, err = NewDataType(DtDuration, NullDuration)
	assert.Nil(t, err)
	assert.True(t, dt.IsNull())
	assert.Equal(t, dt.String(), "")
}

func TestDecimal128(t *testing.T) {
	dt, err := NewDataType(DtDecimal128, &Decimal128{Scale: 0, Value: "123.2"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "123")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 0, Value: "-123.2"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-123")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 0, Value: "0"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "0")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 18, Value: "1.2312"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "1.231200000000000000")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 18, Value: "-1.2312"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-1.231200000000000000")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 19, Value: "1.2312"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "1.2312000000000000000")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 19, Value: "-1.2312"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-1.2312000000000000000")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 19, Value: "1.2312000000000000001123456"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "1.2312000000000000001")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 30, Value: "99999999.999999999999999999999999999999"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "99999999.999999999999999999999999999999")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 30, Value: "99999999.000000000000000000000000000009"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "99999999.000000000000000000000000000009")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 30, Value: "-99999999.999999999999999999999999999999"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-99999999.999999999999999999999999999999")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 30, Value: "-99999999.000000000000000000000000000009"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-99999999.000000000000000000000000000009")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 37, Value: "9.9999999999999999999999999999999999999"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "9.9999999999999999999999999999999999999")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 37, Value: "9.9999999000000000000000000000000000009"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "9.9999999000000000000000000000000000009")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 37, Value: "-9.9999999999999999999999999999999999999"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-9.9999999999999999999999999999999999999")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 37, Value: "-9.9999999000000000000000000000000000009"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "-9.9999999000000000000000000000000000009")

	dt, err = NewDataType(DtDecimal128, &Decimal128{Scale: 6, Value: "103"})
	assert.Nil(t, err)
	assert.Equal(t, dt.String(), "103.000000")
}
