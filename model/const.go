package model

import (
	"math"
	"time"
)

const (
	// MinInt8 is the minimum int8 of type uint8.
	MinInt8 uint8 = 128
	// MinInt32 is minimum int32 of type uint32.
	MinInt32 uint32 = 2147483648
)

const void = "void(null)"

const (
	// LOGICAL is the string type of category LOGICAL.
	LOGICAL CategoryString = "LOGICAL"
	// NOTHING is the string type of category NOTHING.
	NOTHING CategoryString = "NOTHING"
	// INTEGRAL is the string type of category INTEGRAL.
	INTEGRAL CategoryString = "INTEGRAL"
	// FLOATING is the string type of category FLOATING.
	FLOATING CategoryString = "FLOATING"
	// TEMPORAL is the string type of category TEMPORAL.
	TEMPORAL CategoryString = "TEMPORAL"
	// LITERAL is the string type of category LITERAL.
	LITERAL CategoryString = "LITERAL"
	// SYSTEM is the string type of category SYSTEM.
	SYSTEM CategoryString = "SYSTEM"
	// MIXED is the string type of category MIXED.
	MIXED CategoryString = "MIXED"
	// BINARY is the string type of category BINARY.
	BINARY CategoryString = "BINARY"
	// ARRAY is the string type of category ARRAY.
	ARRAY CategoryString = "ARRAY"
	// DECIMAL is the string type of category ARRAY.
	DENARY CategoryString = "DENARY"
)

const (
	// DfScalar is the byte type of Scalar.
	DfScalar DataFormByte = iota
	// DfVector is the byte type of Vector.
	DfVector
	// DfPair is the byte type of Pair.
	DfPair
	// DfMatrix is the byte type of Matrix.
	DfMatrix
	// DfSet is the byte type of Set.
	DfSet
	// DfDictionary is the byte type of Dictionary.
	DfDictionary
	// DfTable is the byte type of Table.
	DfTable
	// DfChart is the byte type of Chart.
	DfChart
	// DfChunk is the byte type of Chunk.
	DfChunk
)

const (
	// DtVoid is the byte type of Void.
	DtVoid DataTypeByte = iota
	// DtBool is the byte type of Bool.
	DtBool
	// DtChar is the byte type of Char.
	DtChar
	// DtShort is the byte type of Short.
	DtShort
	// DtInt is the byte type of Int.
	DtInt
	// DtLong is the byte type of Long.
	DtLong
	// DtDate is the byte type of Date.
	DtDate
	// DtMonth is the byte type of Month.
	DtMonth
	// DtTime is the byte type of Time.
	DtTime
	// DtMinute is the byte type of Minute.
	DtMinute
	// DtSecond is the byte type of Second.
	DtSecond
	// DtDatetime is the byte type of Datetime.
	DtDatetime
	// DtTimestamp is the byte type of Timestamp.
	DtTimestamp
	// DtNanoTime is the byte type of NanoTime.
	DtNanoTime
	// DtNanoTimestamp is the byte type of NanoTimestamp.
	DtNanoTimestamp
	// DtFloat is the byte type of Float.
	DtFloat
	// DtDouble is the byte type of Double.
	DtDouble
	// DtSymbol is the byte type of Symbol.
	DtSymbol
	// DtString is the byte type of String.
	DtString
	// DtUUID is the byte type of UUID.
	DtUUID
	// DtFunction is the byte type of Function.
	DtFunction
	// DtHandle is the byte type of Handle.
	DtHandle
	// DtCode is the byte type of Code.
	DtCode
	// DtDatasource is the byte type of Datasource.
	DtDatasource
	// DtResource is the byte type of Resource.
	DtResource
	// DtAny is the byte type of Any.
	DtAny
	// DtCompress is the byte type of Compress.
	DtCompress
	// DtDictionary is the byte type of Dictionary.
	DtDictionary
	// DtDateHour is the byte type of DateHour.
	DtDateHour
	// DtDateMinute is the byte type of DateMinute.
	DtDateMinute
	// DtIP is the byte type of IP.
	DtIP
	// DtInt128 is the byte type of Int128.
	DtInt128
	// DtBlob is the byte type of Blob.
	DtBlob
	dt33
	// DtComplex is the byte type of Complex.
	DtComplex
	// DtPoint is the byte type of Point.
	DtPoint
	// DtDuration is the byte type of Duration.
	DtDuration
	// DtDecimal32 is the byte type of Decimal32.
	DtDecimal32
	// DtDecimal64 is the byte type of Decimal64.
	DtDecimal64
	// DtDecimal128 is the byte type of Decimal128.
	DtDecimal128
	// DtObject is the byte type of Object.
	DtObject
)

// CategoryString is the string type of Category.
type CategoryString string

// DataTypeByte is the byte type of DataType.
type DataTypeByte byte

// DataFormByte is the byte type of DataForm.
type DataFormByte byte

var (
	emptyTime      = time.Time{}
	emptyDuration  = [2]uint32{MinInt32, 0}
	emptyInt64List = [2]uint64{0, 0}
	emptyPoint     = "(,)"
)

var (
	// Null value for DtString, DtSymbol.
	NullString = ""
	// Null value for DtAny.
	NullAny = nullDataForm
	// Null value for DtUUID.
	NullUUID = "00000000-0000-0000-0000-000000000000"
	// Null value for DtInt128.
	NullInt128 = "00000000000000000000000000000000"
	// Null value for DtIP.
	NullIP = "0.0.0.0"
	// Null value for DTShort.
	NullShort = int16(math.MinInt16)
	// Null value for DtDate,DtDateHour,DtDatetime,DtMinute,DtNanoTime,DtNanoTimestamp,DtSecond,DtMonth,DtTimestamp.
	NullTime = emptyTime
	// Null value for DtLong.
	NullLong = int64(math.MinInt64)
	// Null value for DtDuration.
	NullDuration = ""
	// Null value for DtFloat.
	NullFloat = float32(-math.MaxFloat32)
	// Null value for DtDouble.
	NullDouble = -math.MaxFloat64
	// Null value for DtDecimal32.
	NullDecimal32Value = float64(NullInt)
	// Null value for DtDecimal64.
	NullDecimal64Value = float64(NullLong)
	maxDecimal32Value  = float64(math.MaxInt32)
	maxDecimal64Value  = float64(math.MaxInt64)
	// Null value for DtInt.
	NullInt = int32(math.MinInt32)
	// Null value for DtComplex.
	NullComplex = [2]float64{-math.MaxFloat64, -math.MaxFloat64}
	// Null value for DtPoint.
	NullPoint = [2]float64{-math.MaxFloat64, -math.MaxFloat64}
	// Null value for DtBlob.
	NullBlob = []byte{}
	// Null value for DtBool.
	NullBool = MinInt8
	// Null value for DtChar.
	NullChar = MinInt8
	// Null value for DtCompress.
	NullCompress = MinInt8
)
