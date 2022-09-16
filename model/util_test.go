package model

import (
	"bytes"
	"testing"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"

	"github.com/stretchr/testify/assert"
)

func TestUtil(t *testing.T) {
	dts := GetDataTypeString(4)
	assert.Equal(t, dts, "int")

	dts = GetDataTypeString(68)
	assert.Equal(t, dts, "intArray")

	dts = GetDataTypeString(145)
	assert.Equal(t, dts, "symbolExtend")

	by := bytes.NewBuffer([]byte{1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 2, 0})
	r := protocol.NewReader(by)

	row, col, err := read2Uint32(r, protocol.LittleEndian)
	assert.Nil(t, err)
	assert.Equal(t, row, uint32(1))
	assert.Equal(t, col, uint32(2))

	row16, col16, err := read2Uint16(r, protocol.LittleEndian)
	assert.Nil(t, err)
	assert.Equal(t, row16, uint16(1))
	assert.Equal(t, col16, uint16(2))

	u64 := stringToUint64("3e8")
	assert.Equal(t, u64, uint64(1000))

	df := GetDataFormString(DfVector)
	assert.Equal(t, df, "vector")

	df = GetDataFormString(DfChart)
	assert.Equal(t, df, "chart")

	df = GetDataFormString(DfChunk)
	assert.Equal(t, df, "chunk")

	df = GetDataFormString(DfScalar)
	assert.Equal(t, df, "scalar")

	df = GetDataFormString(DfPair)
	assert.Equal(t, df, "pair")

	df = GetDataFormString(DfDictionary)
	assert.Equal(t, df, "dictionary")

	df = GetDataFormString(DfMatrix)
	assert.Equal(t, df, "matrix")

	df = GetDataFormString(DfSet)
	assert.Equal(t, df, "set")

	df = GetDataFormString(DfTable)
	assert.Equal(t, df, "table")

	cat := GetCategory(DtTime)
	assert.Equal(t, cat, TEMPORAL)

	cat = GetCategory(DtBlob)
	assert.Equal(t, cat, SYSTEM)

	cat = GetCategory(DtInt)
	assert.Equal(t, cat, INTEGRAL)

	cat = GetCategory(DtBool)
	assert.Equal(t, cat, LOGICAL)

	cat = GetCategory(DtFloat)
	assert.Equal(t, cat, FLOATING)

	cat = GetCategory(DtString)
	assert.Equal(t, cat, LITERAL)

	cat = GetCategory(DtInt128)
	assert.Equal(t, cat, BINARY)

	cat = GetCategory(DtAny)
	assert.Equal(t, cat, MIXED)

	srcDt, err := NewDataType(DtTime, time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC))
	assert.Nil(t, err)

	sca := NewScalar(srcDt)
	_, err = CastDateTime(sca, DtTime)
	assert.Equal(t, "the data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATE or DATETIME", err.Error())

	srcDt, err = NewDataType(DtNanoTimestamp, time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC))
	assert.Nil(t, err)

	sca = NewScalar(srcDt)
	res, err := CastDateTime(sca, DtTime)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfScalar)
	assert.Equal(t, res.String(), "time(01:01:01.000)")

	srcDt, err = NewDataType(DtTimestamp, time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC))
	assert.Nil(t, err)

	sca = NewScalar(srcDt)
	res, err = CastDateTime(sca, DtTime)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfScalar)
	assert.Equal(t, res.String(), "time(01:01:01.000)")

	srcDt, err = NewDataType(DtDatetime, time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC))
	assert.Nil(t, err)

	sca = NewScalar(srcDt)
	res, err = CastDateTime(sca, DtTime)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfScalar)
	assert.Equal(t, res.String(), "time(01:01:01.000)")

	srcDt, err = NewDataType(DtDate, time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC))
	assert.Nil(t, err)

	sca = NewScalar(srcDt)
	res, err = CastDateTime(sca, DtTime)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfScalar)
	assert.Equal(t, res.String(), "time(00:00:00.000)")

	srcDtl, err := NewDataTypeListWithRaw(DtTime, []time.Time{time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	vct := NewVector(srcDtl)
	_, err = CastDateTime(vct, DtTime)
	assert.Equal(t, "the data type of the source data must be NANOTIMESTAMP, TIMESTAMP, DATE or DATETIME", err.Error())

	srcDtl, err = NewDataTypeListWithRaw(DtNanoTimestamp, []time.Time{time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	vct = NewVector(srcDtl)
	res, err = CastDateTime(vct, DtDate)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfVector)
	assert.Equal(t, res.String(), "vector<date>([2022.01.01])")

	srcDtl, err = NewDataTypeListWithRaw(DtTimestamp, []time.Time{time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	vct = NewVector(srcDtl)
	res, err = CastDateTime(vct, DtMonth)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfVector)
	assert.Equal(t, res.String(), "vector<month>([2022.01M])")

	srcDtl, err = NewDataTypeListWithRaw(DtDatetime, []time.Time{time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	vct = NewVector(srcDtl)
	res, err = CastDateTime(vct, DtMonth)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfVector)
	assert.Equal(t, res.String(), "vector<month>([2022.01M])")

	srcDtl, err = NewDataTypeListWithRaw(DtDate, []time.Time{time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	vct = NewVector(srcDtl)
	res, err = CastDateTime(vct, DtDateHour)
	assert.Nil(t, err)
	assert.Equal(t, res.GetDataForm(), DfVector)
	assert.Equal(t, res.String(), "vector<dateHour>([2022.01.01T00])")
}
