package model

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/shopspring/decimal"
)

// Vector is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Vector/index.html for more details.
type Vector struct {
	category *Category
	scale    int32

	RowCount    uint32
	ColumnCount uint32

	// If the DataTypeByte of the Vector is less than 64, Data stores the values of the vector.
	// If the DataTypeByte of the Vector is greater than 64 and less than 128, Data is invalid.
	// If the DataTypeByte of the Vector is greater than 128, Data stores the indexs of the values and
	// the value are stored in the Base of Extend.
	// You can call GetDataType() to get the DataTypeByte of the Vector.
	Data DataTypeList
	// ArrayVector is a special form of data for DolphinDB. Unlike a regular vector,
	// each of its elements is an array with the same DataType, but the length can vary.
	// ArrayVector is only valid when the DataTypeByte of the Vector is greater than 64 and less than 128.
	// You can call GetDataType() to get the DataTypeByte of the Vector.
	ArrayVector []*ArrayVector
	// Extend is only valid when the DataTypeByte of the Vector is greater than 128.
	// The Base of the Extend stores the values of the vector.
	// You can call GetDataType() to get the DataTypeByte of the Vector.
	Extend *DataTypeExtend
}

// ArrayVector is an element type of Vector.
type ArrayVector struct {
	rowCount uint16
	unit     uint16
	lengths  []byte

	data DataTypeList
}

// DataTypeExtend is only valid for Symbol DataType.
type DataTypeExtend struct {
	BaseID   uint32
	BaseSize uint32

	Base DataTypeList
}

// NewVector returns an object of vector with specified data.
// You can instantiate the data by NewDataTypeList or NewDataTypeListWithRaw.
func NewVector(data DataTypeList) *Vector {
	return &Vector{
		category: &Category{
			DataForm: DfVector,
			DataType: data.DataType(),
		},
		Data:        data,
		ColumnCount: 1,
		RowCount:    uint32(data.Len()),
	}
}

// NewVectorWithArrayVector returns an object of vector according to the data.
// You can instantiates the data by NewArrayVector.
func NewVectorWithArrayVector(data []*ArrayVector) *Vector {
	if len(data) == 0 {
		return nil
	}

	dt := data[0].data.DataType() + 64
	if dt == 81 || dt == 82 {
		return nil
	}

	res := &Vector{
		category: &Category{
			DataForm: DfVector,
			DataType: dt,
		},
		ArrayVector: data,
		ColumnCount: uint32(len(data)),
	}

	if dt == 101 || dt == 102 || dt == 103 {
		res.scale = getScale(data[0].data.(*dataTypeList))
	}

	for _, v := range data {
		res.RowCount += uint32(v.rowCount)
	}

	res.category = newCategory(byte(DfVector), byte(dt))
	return res
}

// NewArrayVector returns an object of ArrayVector with specified data.
// You can initialize the data by using NewDataTypeList or NewDataTypeListWithRaw.
func NewArrayVector(vl []*Vector) []*ArrayVector {
	res := make([]*ArrayVector, len(vl))
	for k, v := range vl {
		av := &ArrayVector{
			data:     v.Data,
			rowCount: 1,
		}

		row := v.Rows()

		av.unit, av.lengths = packArrayVector(av.rowCount, uint32(row))
		res[k] = av
	}

	return res
}

// GetDataForm returns the byte type of the DataForm.
func (vct *Vector) GetDataForm() DataFormByte {
	return DfVector
}

// Set sets DataType of the vct with ind.
// ArrayVector does not support Set.
// If ind >= len(vct.Data), return an error,
// otherwise cover the original value.
func (vct *Vector) Set(ind int, d DataType) error {
	if vct.Extend != nil {
		if vct.Extend.Base == nil || vct.Extend.Base.Len() == 0 {
			return nil
		}

		ind := vct.Extend.Base.AsOf(d)
		d = &dataType{
			t:    DtInt,
			bo:   protocol.LittleEndian,
			data: int32(ind),
		}
	}

	return vct.Data.Set(ind, d)
}

// Get gets DataType from vct.
// If ind exceeds the size of Vector, return nil.
// ArrayVector does not support Combine.
func (vct *Vector) Get(ind int) DataType {
	if ind >= vct.Rows()*int(vct.ColumnCount) && vct.ArrayVector == nil {
		return nil
	}

	switch {
	case vct.Extend != nil:
		if vct.Extend.Base == nil || vct.Extend.Base.Len() == 0 {
			return &dataType{
				t:    DtString,
				bo:   protocol.LittleEndian,
				data: "",
			}
		}

		raw := vct.Data.ElementValue(ind)
		if raw == nil {
			return nil
		}

		return vct.Extend.Base.Get(int(raw.(int32)))
	case vct.Data != nil:
		return vct.Data.Get(ind)
	case vct.ArrayVector != nil:
		for _, v := range vct.ArrayVector {
			rc := v.data.Len()
			if ind < rc {
				return v.data.Get(ind)
			}

			ind -= rc
		}
	}

	return nil
}

func (arrayVector *ArrayVector) formNewLength() []int32 {
	ret := make([]int32, 0)
	if arrayVector.unit == 1 {
		mid := protocol.Int8SliceFromByteSlice(arrayVector.lengths);
		for _,v := range mid {
			ret = append(ret, int32(v))
		}
		return ret
	} else if arrayVector.unit == 2 {
		mid := protocol.Int16SliceFromByteSlice(arrayVector.lengths);
		for _,v := range mid {
			ret = append(ret, int32(v))
		}
		return ret
	}
    return protocol.Int32SliceFromByteSlice(arrayVector.lengths);
}
// GetVectorValue returns the element of the ArrayVector based on the ind.
func (vct *Vector) GetVectorValue(ind int) *Vector {
	if ind >= vct.Rows() {
		return nil
	}

	if vct.ArrayVector != nil {
		for _, v := range vct.ArrayVector {
			rc := int(v.rowCount)
			if ind < rc {
				st := 0
				newLengths := v.formNewLength()
				for k, l := range newLengths {
					if k == ind {
						return NewVector(v.data.Sub(st, st+int(l)))
					}

					st += int(l)
				}
			}

			ind -= rc
		}
	}

	return nil
}

// AppendVectorValue appends the vector to arrayVector.
func (vct *Vector) AppendVectorValue(data *Vector) (err error) {
	if(vct.category.DataType != data.GetDataType() + 64) {
		return fmt.Errorf("mismatched type, expect %s actual %s", GetDataTypeString(vct.category.DataType-64), data.GetDataTypeString())
	}
	arrayVec := NewArrayVector([]*Vector{data})
	vct.ArrayVector = append(vct.ArrayVector, arrayVec...)
	vct.ColumnCount += 1
	vct.RowCount += 1
	return nil
}

// GetDataType returns the byte type of the DataType.
func (vct *Vector) GetDataType() DataTypeByte {
	return vct.category.DataType
}

// Append appends the DataType to the vector.
// ArrayVector not support append value
func (vct *Vector) Append(value DataType) (err error) {
	switch {
	case vct.Extend != nil:
		if vct.Extend.Base == nil {
			vct.Extend.Base, err = NewDataTypeListFromRawData(DtString, []string{""})
			if err != nil {
				return err
			}
		}

		ind := -1
		strs := vct.Extend.Base.StringList()
		for k, v := range strs {
			if v == value.String() {
				ind = k
			}
		}
		if ind == -1 {
			vct.Extend.Base = vct.Extend.Base.Append(value)
			vct.Data.Append(&dataType{t: DtInt, data: int32(vct.Extend.Base.Len() - 1)})
			return
		}

		vct.Data.Append(&dataType{t: DtInt, data: int32(ind)})
	case vct.Data != nil:
		vct.Data.Append(value)
	}

	vct.RowCount++

	return nil
}

// Combine combines two Vectors and returns a new one.
// ArrayVector does not support Combine.
func (vct *Vector) Combine(in *Vector) (*Vector, error) {
	if vct.Extend != nil {
		if in.Extend == nil {
			return nil, errors.New("invalid vector, the Extend of input cannot be nil")
		}

		indMap, nb := combineBase(vct, in)
		data := combineData(indMap, vct, in)
		return &Vector{
			category:    &Category{DataForm: DfVector, DataType: DtSymbol + 128},
			ColumnCount: 1,
			RowCount:    vct.RowCount + in.RowCount,
			Extend: &DataTypeExtend{
				BaseID:   vct.Extend.BaseID,
				BaseSize: uint32(nb.count),
				Base:     nb,
			},
			Data: data,
		}, nil
	} else if vct.ArrayVector == nil {
		dtl, err := vct.Data.combine(in.Data)
		if err != nil {
			return nil, err
		}

		return NewVector(dtl), nil
	}

	return nil, errors.New("ArrayVector does not support Combine")
}

func combineData(indMap map[int]int, vct, in *Vector) *dataTypeList {
	od := vct.Data.(*dataTypeList)
	idt := in.Data.(*dataTypeList)
	d := &dataTypeList{
		t:       DtInt,
		bo:      protocol.LittleEndian,
		count:   od.Len() + idt.Len(),
		intData: make([]int32, od.Len(), od.Len()+idt.Len()),
	}

	copy(d.intData, od.intData)

	for _, v := range idt.intData {
		d.intData = append(d.intData, int32(indMap[int(v)]))
	}

	return d
}

func combineBase(vct, in *Vector) (map[int]int, *dataTypeList) {
	l := vct.Extend.Base.Len()
	obd := vct.Extend.Base.(*dataTypeList)
	ibd := in.Extend.Base.(*dataTypeList)

	nb := &dataTypeList{
		t:          DtString,
		bo:         protocol.LittleEndian,
		count:      l,
		stringData: make([]string, l),
	}

	copy(nb.stringData, obd.stringData)

	indMap := make(map[int]int)
	for k, v := range ibd.stringData {
		if ind, ok := contains(obd.stringData, v); ok {
			indMap[k] = ind
		} else {
			indMap[k] = nb.count
			nb.stringData = append(nb.stringData, v)
			nb.count++
		}
	}

	return indMap, nb
}

func getScale(dt *dataTypeList) int32 {
	if dt.t == DtDecimal32 {
		return dt.decimal32Data[0]
	} else if dt.t == DtDecimal64 {
		return int32(dt.decimal64Data[0])
	} else if dt.t == DtDecimal128 {
		return dt.decimal128Data.scale
	}

	return 0
}

// SetNull sets the value of DataType in vector to null based on ind.
// ArrayVector does not support SetNull.
func (vct *Vector) SetNull(ind int) {
	switch {
	case vct.Extend != nil:
		if vct.Extend.Base == nil || vct.Extend.Base.Len() == 0 {
			return
		}

		if err := vct.Data.Set(ind, &dataType{t: DtInt, data: int32(0)}); err != nil {
			return
		}
	case vct.Data != nil:
		vct.Data.SetNull(ind)
	case vct.ArrayVector != nil:
	}
}

// IsNull checks whether the value of DataType in vector is null based on the index.
// ArrayVector does not support IsNull.
func (vct *Vector) IsNull(ind int) bool {
	switch {
	case vct.Extend != nil:
		if vct.Extend.Base == nil || vct.Extend.Base.Len() == 0 {
			return true
		}

		return vct.Data.ElementString(ind) == "0"
	case vct.Data != nil:
		return vct.Data.IsNull(ind)
	case vct.ArrayVector != nil:
		for _, av := range vct.ArrayVector {
			if ind < av.data.Len() {
				return av.data.IsNull(ind)
			}

			ind -= av.data.Len()
		}
	}

	return true
}

// Rows returns the row num of the DataForm.
func (vct *Vector) Rows() int {
	return int(vct.RowCount)
}

// HashBucket calculates the hash with the bucket and the value whose index is ind in vct.
func (vct *Vector) HashBucket(ind, bucket int) int {
	if vct.Data != nil {
		dt := vct.Data.Get(ind)
		if dt.DataType() == DtAny {
			sca := dt.Value().(*Scalar)
			dt = sca.DataType
		}

		return dt.HashBucket(bucket)
	}

	return 0
}

// AsOf returns the index of the d in vct.
// ArrayVector does not support AsOf.
// If d is not in vct, returns -1.
func (vct *Vector) AsOf(d DataType) int {
	return vct.Data.AsOf(d)
}

// Render serializes the DataForm with bo and input it into w.
func (vct *Vector) Render(w *protocol.Writer, bo protocol.ByteOrder) error {
	err := vct.category.render(w)
	if err != nil {
		return err
	}

	err = vct.renderLength(w, bo)
	if err != nil {
		return err
	}

	switch {
	case vct.category.DataType > 128:
		err = vct.renderExtend(w, bo)
	case vct.category.DataType > 64:
		err = vct.renderArrayVector(w, bo)
	default:
		err = vct.renderData(w, bo)
	}

	return err
}

// GetSubvector instantiates a Vector with the values in indexes.
// The specified indexes should be less than the length of Vector.
func (vct *Vector) GetSubvector(indexes []int) *Vector {
	var res *Vector
	dt := vct.GetDataType()
	switch {
	case dt < 64:
		res = NewVector(vct.Data.GetSubList(indexes))
	case dt > 64 && dt < 128:
		res = vct.getArrayVectorSubVector(indexes)
	case dt > 128:
		res = NewVector(vct.Data.GetSubList(indexes))
		res.Extend = vct.Extend
		res.category = vct.category
	}

	return res
}

func (vct *Vector) getArrayVectorSubVector(indexes []int) *Vector {
	rawVec := make([]*Vector, 0, len(indexes))
	for _,v := range indexes {
		rawVec = append(rawVec, vct.GetVectorValue(v))
	}
	newData := NewArrayVector(rawVec)
	return NewVectorWithArrayVector(newData)
}

// GetDataTypeString returns the string format of the DataType.
func (vct *Vector) GetDataTypeString() string {
	return GetDataTypeString(vct.category.DataType)
}

// GetDataFormString returns the string format of the DataForm.
func (vct *Vector) GetDataFormString() string {
	return GetDataFormString(vct.category.DataForm)
}

// String returns the string of the DataForm.
func (vct *Vector) String() string {
	by := strings.Builder{}

	if data := vct.formatString(); data != nil {
		by.WriteString(fmt.Sprintf("vector<%s>([%s])", GetDataTypeString(vct.category.DataType), strings.Join(data, ", ")))
	} else {
		by.WriteString(fmt.Sprintf("vector<%s>(null)", GetDataTypeString(vct.category.DataType)))
	}

	return by.String()
}

func (vct *Vector) renderArrayVector(w *protocol.Writer, bo protocol.ByteOrder) error {
	dt := vct.GetDataType() - 64
	if dt == DtDecimal32 || dt == DtDecimal64 || dt == DtDecimal128 {
		err := vct.renderDecimalArrayVector(w, bo)
		if err != nil {
			return err
		}
	} else {
		err := vct.renderCommonArrayVector(w, bo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (vct *Vector) renderDecimalArrayVector(w *protocol.Writer, bo protocol.ByteOrder) error {
	buf := make([]byte, 4)
	bo.PutUint32(buf, uint32(vct.scale))
	err := w.Write(buf)
	if err != nil {
		return err
	}

	for _, avt := range vct.ArrayVector {
		bo.PutUint16(buf[0:2], avt.rowCount)
		bo.PutUint16(buf[2:4], avt.unit)
		err := w.Write(buf)
		if err != nil {
			return err
		}

		if avt.data.Len() > 0 {
			err = w.Write(avt.lengths)
			if err != nil {
				return err
			}
		}

		err = writeDecimalArrayVector(w, avt.data.(*dataTypeList), vct.scale)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeDecimalArrayVector(w *protocol.Writer, d *dataTypeList, scale int32) error {
	var err error
	switch d.t {
	case DtDecimal32:
		err = writeDecimal32Data(w, d, scale)
	case DtDecimal64:
		err = writeDecimal64Data(w, d, int64(scale))
	case DtDecimal128:
		err = writeDecimal128Data(w, d, scale)
	}

	return err
}

func writeDecimal32Data(w *protocol.Writer, d *dataTypeList, scale int32) error {
	data := make([]int32, len(d.decimal32Data[1:]))
	copy(data, d.decimal32Data[1:])
	if scale != d.decimal32Data[0] {
		for k, v := range data {
			if v != NullInt {
				data[k] = int32(float64(v) * math.Pow10(int(scale-d.decimal32Data[0])))
			}
		}
	}

	return w.Write(protocol.ByteSliceFromInt32Slice(data))
}

func writeDecimal64Data(w *protocol.Writer, d *dataTypeList, scale int64) error {
	data := make([]int64, len(d.decimal64Data[1:]))
	copy(data, d.decimal64Data[1:])
	if scale != d.decimal64Data[0] {
		for k, v := range data {
			if v != NullLong {
				data[k] = int64(float64(v) * math.Pow10(int(scale-d.decimal64Data[0])))
			}
		}
	}

	return w.Write(protocol.ByteSliceFromInt64Slice(data))
}

func writeDecimal128Data(w *protocol.Writer, d *dataTypeList, scale int32) error {
	data := make([]byte, 16*len(d.decimal128Data.value))
	if scale != d.decimal128Data.scale {
		for k, v := range d.decimal128Data.value {
			if v.Cmp(minBigIntValue) != 0 {
				divNum, _ := decimal.NewFromString(fmt.Sprintf("1e+%d", d.decimal128Data.scale))
				subNum, _ := decimal.NewFromString(fmt.Sprintf("1e+%d", scale))
				dec := decimal.NewFromBigInt(v, 0).DivRound(divNum, d.decimal128Data.scale).Mul(subNum)
				err := fullBigIntBytes(data, dec.BigInt(), 16*k)
				if err != nil {
					return err
				}
			} else {
				err := fullBigIntBytes(data, v, 16*k)
				if err != nil {
					return err
				}
			}
		}
	} else {
		for k, v := range d.decimal128Data.value {
			err := fullBigIntBytes(data, v, 16*k)
			if err != nil {
				return err
			}
		}
	}

	reverseByteArrayEvery8Byte(data)

	return w.Write(data)
}

func (vct *Vector) renderCommonArrayVector(w *protocol.Writer, bo protocol.ByteOrder) error {
	buf := make([]byte, 4)
	for _, avt := range vct.ArrayVector {
		bo.PutUint16(buf[0:2], avt.rowCount)
		bo.PutUint16(buf[2:4], avt.unit)
		err := w.Write(buf)
		if err != nil {
			return err
		}

		if avt.data.Len() > 0 {
			err = w.Write(avt.lengths)
			if err != nil {
				return err
			}
		}

		err = avt.data.Render(w, bo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (vct *Vector) renderExtend(w *protocol.Writer, bo protocol.ByteOrder) error {
	ext := vct.Extend
	buf := make([]byte, 8)
	bo.PutUint32(buf[0:4], ext.BaseID)
	bo.PutUint32(buf[4:8], uint32(ext.Base.Len()))
	err := w.Write(buf)
	if err != nil {
		return err
	}

	if ext.BaseSize != 0 {
		err = ext.Base.Render(w, bo)
		if err != nil {
			return err
		}
	}

	err = vct.Data.Render(w, bo)
	if err != nil {
		return err
	}

	return nil
}

func (vct *Vector) renderSymbolExtendVector(w *protocol.Writer, bo protocol.ByteOrder, symBases *symbolBaseCollection) error {
	err := vct.category.render(w)
	if err != nil {
		return err
	}

	err = vct.renderLength(w, bo)
	if err != nil {
		return err
	}

	err = symBases.write(w, bo, vct.Extend)
	if err != nil {
		return err
	}

	err = vct.Data.Render(w, bo)
	if err != nil {
		return err
	}

	return nil
}

func (vct *Vector) renderLength(w *protocol.Writer, bo protocol.ByteOrder) error {
	buf := make([]byte, 8)
	bo.PutUint32(buf[0:4], vct.RowCount)
	bo.PutUint32(buf[4:8], vct.ColumnCount)

	return w.Write(buf)
}

func (vct *Vector) renderData(w *protocol.Writer, bo protocol.ByteOrder) error {
	return vct.Data.Render(w, bo)
}

// GetRawValue returns an array of values of the elements in the Vector.
func (vct *Vector) GetRawValue() []interface{} {
	res := make([]interface{}, 0, vct.RowCount*vct.ColumnCount)
	switch {
	case vct.Extend != nil:
		d := vct.Data.(*dataTypeList)
		sl := vct.Extend.Base.StringList()
		for _, v := range d.intData {
			res = append(res, sl[v])
		}
	case vct.Data != nil:
		res = vct.Data.Value()
	case vct.ArrayVector != nil:
		for _, v := range vct.ArrayVector {
			asl := v.data.Value()
			si := 0
			newLengths := v.formNewLength()
			for _, l := range newLengths {
				length := int(l)
				res = append(res, asl[si:si+length])
				si += length
			}
		}
	}

	return res
}

func (vct *Vector) formatString() []string {
	val := make([]string, 0)
	switch {
	case vct.Extend != nil:
		d := vct.Data.(*dataTypeList)
		sl := vct.Extend.Base.StringList()
		for _, v := range d.intData {
			val = append(val, sl[v])
		}
	case vct.Data != nil:
		val = vct.Data.StringList()
	case len(vct.ArrayVector) > 0:
		for _, v := range vct.ArrayVector {
			asl := v.data.StringList()
			si := 0
			newLengths := v.formNewLength()
			for _, l := range newLengths {
				length := int(l)
				val = append(val, fmt.Sprintf("[%s]", strings.Join(asl[si:si+length], ", ")))
				si += length
			}
		}
	}

	return val
}

func packArrayVector(rowCount uint16, length uint32) (uint16, []byte) {
	switch {
	case length < math.MaxUint8:
		res := make([]int8, rowCount)
		for i := 0; i < int(rowCount); i++ {
			res[i] = int8(length)
		}

		return 1, protocol.ByteSliceFromInt8Slice(res)
	case length < math.MaxUint16:
		res := make([]int16, rowCount)
		for i := 0; i < int(rowCount); i++ {
			res[i] = int16(length)
		}

		return 2, protocol.ByteSliceFromInt16Slice(res)
	default:
		res := make([]int32, rowCount)
		for i := 0; i < int(rowCount); i++ {
			res[i] = int32(length)
		}

		return 4, protocol.ByteSliceFromInt32Slice(res)
	}
}
