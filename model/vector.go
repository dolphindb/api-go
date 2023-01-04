package model

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/dolphindb/api-go/dialer/protocol"
)

// Vector is a DataForm.
// Refer to https://www.dolphindb.cn/cn/help/130/DataTypesandStructures/DataForms/Vector/index.html for more details.
type Vector struct {
	category *Category

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
		if vct.Extend.BaseSize == 0 {
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
func (vct *Vector) Get(ind int) DataType {
	if ind >= vct.Rows()*int(vct.ColumnCount) && vct.ArrayVector == nil {
		return nil
	}

	switch {
	case vct.Extend != nil:
		if vct.Extend.BaseSize == 0 {
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
				for k, l := range v.lengths {
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

// GetDataType returns the byte type of the DataType.
func (vct *Vector) GetDataType() DataTypeByte {
	return vct.category.DataType
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

// SetNull sets the value of DataType in vector to null based on ind.
// ArrayVector does not support SetNull.
func (vct *Vector) SetNull(ind int) {
	switch {
	case vct.Extend != nil:
		if vct.Extend.BaseSize == 0 {
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
		if vct.Extend.BaseSize == 0 {
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
// ArrayVector does not support GetSubvector.
// The specified indexes should be less than the length of Vector.
func (vct *Vector) GetSubvector(indexes []int) *Vector {
	if vct.Data == nil {
		return nil
	}

	res := NewVector(vct.Data.GetSubList(indexes))
	if vct.Extend != nil {
		res.Extend = vct.Extend
		res.category = vct.category
	}

	return res
}

// GetDataTypeString returns the string format of the DataType.
func (vct *Vector) GetDataTypeString() string {
	return GetDataTypeString(vct.category.DataType)
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
	for _, v := range vct.ArrayVector {
		buf := make([]byte, 4)

		bo.PutUint16(buf[0:2], v.rowCount)
		bo.PutUint16(buf[2:4], v.unit)
		err := w.Write(buf)
		if err != nil {
			return err
		}

		if v.data.Len() > 0 {
			err = w.Write(v.lengths)
			if err != nil {
				return err
			}
		}

		err = v.data.Render(w, bo)
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
	bo.PutUint32(buf[4:8], ext.BaseSize)
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

func (vct *Vector) renderLength(w *protocol.Writer, bo protocol.ByteOrder) error {
	buf := make([]byte, 8)
	bo.PutUint32(buf[0:4], vct.RowCount)
	bo.PutUint32(buf[4:8], vct.ColumnCount)

	return w.Write(buf)
}

func (vct *Vector) renderData(w *protocol.Writer, bo protocol.ByteOrder) error {
	return vct.Data.Render(w, bo)
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
			for _, l := range v.lengths {
				length := int(l)
				val = append(val, fmt.Sprintf("[%s]", strings.Join(asl[si:si+length], ", ")))
				si += length
			}
		}
	}

	return val
}

func packArrayVector(rowcount uint16, length uint32) (uint16, []byte) {
	switch {
	case length < math.MaxUint8:
		res := make([]int8, rowcount)
		for i := 0; i < int(rowcount); i++ {
			res[i] = int8(length)
		}

		return 1, protocol.ByteSliceFromInt8Slice(res)
	case length < math.MaxUint16:
		res := make([]int16, rowcount)
		for i := 0; i < int(rowcount); i++ {
			res[i] = int16(length)
		}

		return 2, protocol.ByteSliceFromInt16Slice(res)
	default:
		res := make([]int32, rowcount)
		for i := 0; i < int(rowcount); i++ {
			res[i] = int32(length)
		}

		return 4, protocol.ByteSliceFromInt32Slice(res)
	}
}
