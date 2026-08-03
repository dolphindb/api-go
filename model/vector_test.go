package model

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/v3/dialer/protocol"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVector(t *testing.T) {
	by := bytes.NewBufferString("")
	w := protocol.NewWriter(by)

	dtl, err := NewDataTypeListFromRawData(DtInt, []int32{0, 1})
	assert.Nil(t, err)
	assert.Equal(t, dtl.DataType(), DtInt)

	vc := NewVector(dtl)
	assert.Equal(t, vc.GetDataForm(), DfVector)
	assert.Equal(t, vc.GetDataType(), DtInt)
	assert.Equal(t, vc.GetDataTypeString(), "int")
	assert.Equal(t, vc.Rows(), 2)
	assert.Equal(t, vc.AsOf(dtl.Get(0)), 0)
	assert.Equal(t, vc.AsOf(dtl.Get(1)), 1)
	assert.Equal(t, vc.HashBucket(0, 10), 0)
	assert.Equal(t, vc.HashBucket(1, 10), 1)

	vc.SetNull(1)
	assert.True(t, vc.IsNull(1))

	err = vc.Set(1, vc.Get(0))
	assert.Nil(t, err)
	assert.Equal(t, vc.Rows(), 2)

	err = vc.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, by.String(), "\x04\x01\x02\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	assert.Equal(t, vc.String(), "vector<int>([0, 0])")

	vc.Data = dtl.Sub(0, 1)
	vc.RowCount = 1
	dtl, err = NewDataTypeListFromRawData(DtString, []string{"vector", "zero"})
	assert.Nil(t, err)

	vc.Extend = &DataTypeExtend{
		BaseID:   10,
		BaseSize: 1,
		Base:     dtl.Sub(1, 2),
	}
	vc.category.DataType = 145

	by.Reset()
	err = vc.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, vc.Rows(), 1)
	assert.Equal(t, by.String(), "\x91\x01\x01\x00\x00\x00\x01\x00\x00\x00\n\x00\x00\x00\x01\x00\x00\x00zero\x00\x00\x00\x00\x00")
	assert.Equal(t, vc.String(), "vector<symbolExtend>([zero])")

	combineDtl, err := NewDataTypeListFromRawData(DtInt, []int32{0, 1, 1, 0})
	assert.Nil(t, err)
	tmp := NewVector(combineDtl)
	tmp.Extend = &DataTypeExtend{
		BaseID:   10,
		BaseSize: 2,
		Base:     dtl,
	}
	tmp.category.DataType = 145
	cv, err := vc.Combine(tmp)
	assert.Nil(t, err)
	assert.Equal(t, cv.String(), "vector<symbolExtend>([zero, vector, zero, zero, vector])")

	dt := vc.Get(0)
	assert.Equal(t, dt.String(), "zero")

	vc.SetNull(0)
	assert.True(t, vc.IsNull(0))

	dtl, err = NewDataTypeListFromRawData(DtInt, []int32{1, 2, 3})
	assert.Nil(t, err)
	assert.Equal(t, dtl.DataType(), DtInt)

	by.Reset()

	vc = NewVector(dtl)
	vc = mustNewVectorWithArrayVector(t, NewArrayVector([]*Vector{vc}))
	err = vc.Render(w, protocol.LittleEndian)
	w.Flush()
	assert.Nil(t, err)
	assert.Equal(t, vc.Rows(), 1)
	assert.Equal(t, by.String(), "D\x01\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x01\x00\x03\x01\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00")
	assert.Equal(t, vc.String(), "vector<intArray>([[1, 2, 3]])")

	v := vc.GetVectorValue(0)
	assert.Equal(t, v.String(), "vector<int>([1, 2, 3])")
	assert.False(t, vc.IsNull(0))

	unit, byt := packArrayVector(4, 30000)
	assert.Equal(t, unit, uint16(2))
	assert.Equal(t, byt, []byte{0x30, 0x75, 0x30, 0x75, 0x30, 0x75, 0x30, 0x75})

	unit, byt = packArrayVector(4, 65536)
	assert.Equal(t, unit, uint16(4))
	assert.Equal(t, byt, []byte{0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1, 0x0})

	vd := NewEmptyDataTypeList(DtVoid, 10)
	vct := NewVector(vd)
	assert.Equal(t, int(vct.RowCount), 10)
	assert.Equal(t, vct.String(), "vector<void>([, , , , , , , , , ])")

	str, err := NewDataType(DtString, "void")
	assert.Nil(t, err)
	err = vct.Set(0, str)
	assert.Nil(t, err)
	assert.Equal(t, vct.Get(0).String(), "")
}

func TestRenderEmptyArrayVectorIncludesLengths(t *testing.T) {
	// AG-183: common and decimal empty ArrayVector elements must still serialize
	// their zero length byte even though they have no payload.
	tests := []struct {
		name  string
		dt    DataTypeByte
		raw   interface{}
		scale *int32
	}{
		{name: "double", dt: DtDouble, raw: []float64{}},
		{name: "decimal32", dt: DtDecimal32, raw: &Decimal32s{Scale: 2, Value: []float64{}}, scale: pointerTo(int32(2))},
		{name: "decimal64", dt: DtDecimal64, raw: &Decimal64s{Scale: 2, Value: []float64{}}, scale: pointerTo(int32(2))},
		{name: "decimal128", dt: DtDecimal128, raw: &Decimal128s{Scale: 2, Value: []string{}}, scale: pointerTo(int32(2))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			empty, err := NewDataTypeListFromRawData(tc.dt, tc.raw)
			require.NoError(t, err)

			av := mustNewVectorWithArrayVector(t, NewArrayVector([]*Vector{NewVector(empty)}))
			require.Equal(t, 1, av.Rows())
			require.Equal(t, 0, av.ArrayVector[0].data.Len())
			require.Equal(t, []byte{0x00}, av.ArrayVector[0].lengths)

			by := bytes.NewBuffer(nil)
			w := protocol.NewWriter(by)
			require.NoError(t, av.Render(w, protocol.LittleEndian))
			require.NoError(t, w.Flush())

			expected := []byte{
				byte(tc.dt + 64), byte(DfVector),
				0x01, 0x00, 0x00, 0x00, // RowCount
				0x01, 0x00, 0x00, 0x00, // ColumnCount
			}
			if tc.scale != nil {
				expected = append(expected, byte(*tc.scale), 0x00, 0x00, 0x00)
			}
			expected = append(expected,
				0x01, 0x00, // array chunk rowCount
				0x01, 0x00, // unit
				0x00, // length of the single empty array element
			)
			require.Equal(t, expected, by.Bytes())

			parsed, err := ParseDataForm(
				protocol.NewReader(bytes.NewReader(by.Bytes())),
				protocol.LittleEndian,
			)
			require.NoError(t, err)
			got, ok := parsed.(*Vector)
			require.True(t, ok)
			require.Equal(t, tc.dt+64, got.GetDataType())
			require.Equal(t, 1, got.Rows())
			require.Equal(t, 0, got.ArrayVector[0].data.Len())
			require.Equal(t, []byte{0x00}, got.ArrayVector[0].lengths)
		})
	}
}

func pointerTo[T any](v T) *T {
	return &v
}
