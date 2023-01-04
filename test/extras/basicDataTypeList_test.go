package test

import (
	"math"
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_DataTypeList(t *testing.T) {
	Convey("Test_DataTypeList", t, func() {
		void, err := model.NewDataType(model.DtVoid, nil)
		So(err, ShouldBeNil)

		dtl := model.NewDataTypeList(model.DtVoid, []model.DataType{void})
		So(dtl.DataType(), ShouldEqual, model.DtVoid)
		So(dtl.IsNull(0), ShouldBeTrue)
		So(dtl.Value(), ShouldResemble, []interface{}{"void(null)"})
		So(dtl.ElementValue(0), ShouldEqual, "void(null)")
		So(dtl.AsOf(void), ShouldEqual, -1)
		So(dtl.Get(0).String(), ShouldEqual, void.String())

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(void)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.Set(0, void)
		So(err, ShouldBeNil)

		err = dtl.SetWithRawData(0, 0)
		So(err, ShouldBeNil)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		b, err := model.NewDataType(model.DtBool, true)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtBool, []model.DataType{b})
		So(dtl.DataType(), ShouldEqual, model.DtBool)
		So(dtl.ElementValue(0), ShouldEqual, true)
		So(dtl.AsOf(void), ShouldEqual, -1)
		So(dtl.AsOf(b), ShouldEqual, -1)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(b)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, false)
		So(err, ShouldBeNil)
		So(dtl.ElementValue(0), ShouldEqual, false)

		dtl.SetNull(0)
		So(dtl.ElementValue(0), ShouldEqual, int8(-128))

		c, err := model.NewDataType(model.DtChar, byte(1))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtChar, []model.DataType{c})
		So(dtl.DataType(), ShouldEqual, model.DtChar)
		So(dtl.ElementValue(0), ShouldEqual, int8(1))
		So(dtl.AsOf(c), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(c)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(1, byte(3))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(c), ShouldEqual, 0)

		s, err := model.NewDataType(model.DtShort, int16(1))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtShort, []model.DataType{s})
		So(dtl.DataType(), ShouldEqual, model.DtShort)
		So(dtl.ElementValue(0), ShouldEqual, int16(1))
		So(dtl.AsOf(s), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(s)
		So(dtl.Len(), ShouldEqual, 2)

		dtl.Append(s)
		So(dtl.Len(), ShouldEqual, 3)

		err = dtl.SetWithRawData(1, int16(2))
		So(err, ShouldBeNil)
		err = dtl.SetWithRawData(2, int16(3))
		So(err, ShouldBeNil)

		So(dtl.AsOf(s), ShouldEqual, 0)

		err = dtl.SetWithRawData(0, int16(1))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		f, err := model.NewDataType(model.DtFloat, float32(1))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtFloat, []model.DataType{f})
		So(dtl.DataType(), ShouldEqual, model.DtFloat)
		So(dtl.ElementValue(0), ShouldEqual, float32(1))
		So(dtl.AsOf(f), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(f)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(1, float32(4))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(f), ShouldEqual, 0)

		d, err := model.NewDataType(model.DtDouble, float64(1))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtDouble, []model.DataType{d})
		So(dtl.DataType(), ShouldEqual, model.DtDouble)
		So(dtl.ElementValue(0), ShouldEqual, float64(1))
		So(dtl.AsOf(d), ShouldEqual, 0)

		dtl.Append(d)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(1, float64(3))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(d), ShouldEqual, 0)

		du, err := model.NewDataType(model.DtDuration, "10H")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtDuration, []model.DataType{du})
		So(dtl.ElementValue(0), ShouldEqual, "10H")
		So(dtl.DataType(), ShouldEqual, model.DtDuration)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(du)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "2147483648H")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)
		So(dtl.Value(), ShouldResemble, []interface{}{"", "10H"})

		err = dtl.Set(0, du)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		sym, err := model.NewDataType(model.DtSymbol, "symbol")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtSymbol, []model.DataType{sym})
		So(dtl.DataType(), ShouldEqual, model.DtSymbol)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.ElementValue(0), ShouldEqual, "symbol")
		So(dtl.ElementValue(1), ShouldBeNil)
		So(dtl.ElementString(1), ShouldEqual, "")
		So(dtl.AsOf(sym), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(sym)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(1, "zero")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(sym), ShouldEqual, 0)

		err = dtl.Set(0, du)
		So(err, ShouldNotBeNil)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		str, err := model.NewDataType(model.DtString, "string")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtString, []model.DataType{str})
		So(dtl.DataType(), ShouldEqual, model.DtString)
		So(dtl.AsOf(str), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(str)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "string")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.Set(0, sym)
		So(err, ShouldBeNil)
		So(dtl.ElementValue(0), ShouldEqual, "symbol")

		date := time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)
		m, err := model.NewDataType(model.DtMonth, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtMonth, []model.DataType{m})
		So(dtl.DataType(), ShouldEqual, model.DtMonth)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "2022.01M")
		So(dtl.AsOf(m), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(m)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.Set(0, sym)
		So(err, ShouldNotBeNil)

		err = dtl.Set(2, sym)
		So(err, ShouldNotBeNil)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		ti, err := model.NewDataType(model.DtTime, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtTime, []model.DataType{ti})
		So(dtl.DataType(), ShouldEqual, model.DtTime)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "01:01:01.000")
		So(dtl.AsOf(ti), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(ti)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		ts, err := model.NewDataType(model.DtTimestamp, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtTimestamp, []model.DataType{ts})
		So(dtl.DataType(), ShouldEqual, model.DtTimestamp)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01T01:01:01.000")
		So(dtl.AsOf(ts), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(ts)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		mi, err := model.NewDataType(model.DtMinute, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtMinute, []model.DataType{mi})
		So(dtl.DataType(), ShouldEqual, model.DtMinute)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "01:01m")
		So(dtl.AsOf(mi), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(mi)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		sec, err := model.NewDataType(model.DtSecond, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtSecond, []model.DataType{sec})
		So(dtl.DataType(), ShouldEqual, model.DtSecond)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "01:01:01")
		So(dtl.AsOf(sec), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(sec)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dt, err := model.NewDataType(model.DtDatetime, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtDatetime, []model.DataType{dt})
		So(dtl.DataType(), ShouldEqual, model.DtDatetime)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01T01:01:01")
		So(dtl.AsOf(dt), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(dt)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dh, err := model.NewDataType(model.DtDateHour, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtDateHour, []model.DataType{dh})
		So(dtl.DataType(), ShouldEqual, model.DtDateHour)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01T01")
		So(dtl.AsOf(dh), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(dh)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		dm, err := model.NewDataType(model.DtDateMinute, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtDateMinute, []model.DataType{dm})
		So(dtl.DataType(), ShouldEqual, model.DtDateMinute)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01T01:01")
		So(dtl.Value(), ShouldNotBeNil)
		So(dtl.AsOf(dm), ShouldEqual, 0)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.StringList(), ShouldResemble, []string{"2022.01.01T01:01"})
		So(dtl.Get(0).String(), ShouldEqual, dm.String())

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(dm)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		err = dtl.Set(0, dm)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01T01:01")

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		l, err := model.NewDataType(model.DtLong, int64(10))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtLong, []model.DataType{l})
		So(dtl.DataType(), ShouldEqual, model.DtLong)
		So(dtl.ElementValue(0), ShouldEqual, int64(10))
		So(dtl.AsOf(l), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(l)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, int64(10))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		nt, err := model.NewDataType(model.DtNanoTime, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtNanoTime, []model.DataType{nt})
		So(dtl.DataType(), ShouldEqual, model.DtNanoTime)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "01:01:01.000000001")
		So(dtl.AsOf(nt), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(nt)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		ip, err := model.NewDataType(model.DtIP, "127.0.0.1")
		So(err, ShouldBeNil)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		dtl = model.NewDataTypeList(model.DtIP, []model.DataType{ip})
		So(dtl.ElementValue(0), ShouldEqual, "127.0.0.1")
		So(dtl.DataType(), ShouldEqual, model.DtIP)
		So(dtl.Value(), ShouldResemble, []interface{}{"127.0.0.1"})

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(ip)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "127.0.0.1")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.SetWithRawData(0, 1)
		So(err, ShouldNotBeNil)

		dtl.SetNull(0)
		So(dtl.ElementString(0), ShouldEqual, "0.0.0.0")
		So(dtl.Value(), ShouldResemble, []interface{}{"0.0.0.0", "127.0.0.1"})

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		nts, err := model.NewDataType(model.DtNanoTimestamp, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtNanoTimestamp, []model.DataType{nts})
		So(dtl.DataType(), ShouldEqual, model.DtNanoTimestamp)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01T01:01:01.000000001")
		So(dtl.AsOf(nts), ShouldEqual, 0)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(nts)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		i, err := model.NewDataType(model.DtInt, int32(1))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtInt, []model.DataType{i})
		So(dtl.DataType(), ShouldEqual, model.DtInt)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "1")

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(i)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, int32(1))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		int128, err := model.NewDataType(model.DtInt128, "e1671797c52e15f763380b45e841ec32")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtInt128, []model.DataType{int128})
		So(dtl.DataType(), ShouldEqual, model.DtInt128)
		So(dtl.ElementValue(0), ShouldEqual, "e1671797c52e15f763380b45e841ec32")
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.Get(2), ShouldBeNil)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(int128)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(1, "000000000000000063380b45e841ec32")
		So(err, ShouldBeNil)
		So(dtl.IsNull(1), ShouldBeFalse)

		err = dtl.SetWithRawData(0, 1)
		So(err, ShouldNotBeNil)

		err = dtl.Set(2, int128)
		So(err, ShouldNotBeNil)

		dtl.SetNull(0)
		So(dtl.ElementString(0), ShouldEqual, "00000000000000000000000000000000")
		So(dtl.Value(), ShouldResemble, []interface{}{"00000000000000000000000000000000", "000000000000000063380b45e841ec32"})

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		dt, err = model.NewDataType(model.DtDate, date)
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtDate, []model.DataType{dt})
		So(dtl.DataType(), ShouldEqual, model.DtDate)
		So(dtl.ElementValue(0), ShouldNotBeNil)
		So(dtl.ElementString(0), ShouldEqual, "2022.01.01")
		So(dtl.AsOf(dt), ShouldEqual, 0)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(dt)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, date)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.Set(0, nil)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeTrue)

		uuid, err := model.NewDataType(model.DtUUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtUUID, []model.DataType{uuid})
		So(dtl.DataType(), ShouldEqual, model.DtUUID)
		So(dtl.ElementValue(0), ShouldEqual, "5d212a78-cc48-e3b1-4235-b4d91473ee87")
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(uuid), ShouldEqual, -1)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(uuid)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "11110000-0000-0000-0000-000000000000")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.SetWithRawData(0, 1)
		So(err, ShouldNotBeNil)

		dtl.SetNull(0)
		So(dtl.ElementString(0), ShouldEqual, "00000000-0000-0000-0000-000000000000")
		So(dtl.Value(), ShouldResemble, []interface{}{"00000000-0000-0000-0000-000000000000", "5d212a78-cc48-e3b1-4235-b4d91473ee87"})

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		cl, err := model.NewDataType(model.DtComplex, [2]float64{1, 1})
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtComplex, []model.DataType{cl})
		So(dtl.DataType(), ShouldEqual, model.DtComplex)
		So(dtl.ElementValue(0), ShouldEqual, "1.00000+1.00000i")
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(cl)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, [2]float64{1, 1})
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.SetWithRawData(1, [2]float64{1. - math.MaxFloat64})
		So(err, ShouldBeNil)
		So(dtl.Value(), ShouldResemble, []interface{}{"1.00000+1.00000i", ""})

		err = dtl.SetWithRawData(2, [2]float64{-math.MaxFloat64, 1})
		So(err, ShouldNotBeNil)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)
		So(dtl.Value(), ShouldResemble, []interface{}{"", ""})

		err = dtl.Set(0, cl)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		po, err := model.NewDataType(model.DtPoint, [2]float64{1, 1})
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtPoint, []model.DataType{po})
		So(dtl.ElementValue(0), ShouldEqual, "(1.00000, 1.00000)")
		So(dtl.DataType(), ShouldEqual, model.DtPoint)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(po)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(2, [2]float64{-math.MaxFloat64, 1})
		So(err, ShouldNotBeNil)

		err = dtl.SetWithRawData(0, 1)
		So(err, ShouldNotBeNil)

		err = dtl.SetWithRawData(0, [2]float64{1, -math.MaxFloat64})
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeTrue)
		So(dtl.Value(), ShouldResemble, []interface{}{"(,)", "(1.00000, 1.00000)"})

		err = dtl.SetWithRawData(1, [2]float64{1, -math.MaxFloat64})
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeTrue)
		So(dtl.Value(), ShouldResemble, []interface{}{"(,)", "(,)"})

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)
		So(dtl.Value(), ShouldResemble, []interface{}{"(,)", "(,)"})
		So(dtl.ElementString(0), ShouldEqual, "(,)")

		err = dtl.Set(0, po)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		code, err := model.NewDataType(model.DtCode, "typestr")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtCode, []model.DataType{code})
		So(dtl.DataType(), ShouldEqual, model.DtCode)
		So(dtl.ElementValue(0), ShouldEqual, "typestr")
		So(dtl.Value(), ShouldResemble, []interface{}{"typestr"})
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(code), ShouldEqual, -1)
		So(dtl.Get(0).String(), ShouldEqual, code.String())

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(code)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "typestr")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		err = dtl.Set(0, code)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		fc, err := model.NewDataType(model.DtFunction, "typestr()")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtFunction, []model.DataType{fc})
		So(dtl.DataType(), ShouldEqual, model.DtFunction)
		So(dtl.ElementValue(0), ShouldEqual, "typestr()")
		So(dtl.Value(), ShouldResemble, []interface{}{"typestr()"})
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.Get(0).String(), ShouldEqual, fc.String())

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(fc)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "typestr")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		err = dtl.Set(0, fc)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		hd, err := model.NewDataType(model.DtHandle, "db")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtHandle, []model.DataType{hd})
		So(dtl.DataType(), ShouldEqual, model.DtHandle)
		So(dtl.ElementValue(0), ShouldEqual, "db")
		So(dtl.Value(), ShouldResemble, []interface{}{"db"})
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.Get(0).String(), ShouldEqual, hd.String())

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(hd)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "db")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		err = dtl.Set(0, hd)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		obj, err := model.NewDataType(model.DtObject, "db")
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtObject, []model.DataType{obj})
		So(dtl.DataType(), ShouldEqual, model.DtObject)
		So(dtl.Value(), ShouldNotBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.Get(0).String(), ShouldEqual, obj.String())
		So(dtl.Sub(-1, 1), ShouldBeNil)
		So(dtl.Sub(0, 2), ShouldBeNil)
		So(dtl.Sub(2, 0), ShouldBeNil)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(obj)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, "db")
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		blob, err := model.NewDataType(model.DtBlob, []byte{1, 1})
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtBlob, []model.DataType{blob})
		So(dtl.DataType(), ShouldEqual, model.DtBlob)
		So(dtl.ElementString(0), ShouldNotBeNil)

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(blob)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, []byte{1, 1})
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		err = dtl.Set(0, blob)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		any, err := model.NewDataType(model.DtAny, model.NewScalar(blob))
		So(err, ShouldBeNil)

		dtl = model.NewDataTypeList(model.DtAny, []model.DataType{any})
		So(dtl.DataType(), ShouldEqual, model.DtAny)
		So(dtl.IsNull(0), ShouldBeFalse)
		So(dtl.AsOf(any), ShouldEqual, -1)
		So(dtl.Get(0).String(), ShouldEqual, any.String())

		dtl = dtl.Sub(0, 1)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.Append(any)
		So(dtl.Len(), ShouldEqual, 2)

		err = dtl.SetWithRawData(0, model.NewScalar(blob))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		err = dtl.Set(0, any)
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl = dtl.GetSubList([]int{0})
		So(dtl.Len(), ShouldEqual, 1)

		dtl = model.NewEmptyDataTypeList(model.DataTypeByte(145), 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtObject, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtVoid, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtDuration, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtDateMinute, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtComplex, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtPoint, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtCode, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtFunction, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtHandle, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtBlob, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl = model.NewEmptyDataTypeList(model.DtAny, 10)
		So(dtl.Len(), ShouldEqual, 10)

		dtl, err = model.NewDataTypeListFromRawData(model.DataTypeByte(145), []string{"symbol"})
		So(err, ShouldBeNil)
		So(dtl.Len(), ShouldEqual, 1)

		dtl, err = model.NewDataTypeListFromRawData(model.DtCompress, []byte{1})
		So(err, ShouldBeNil)
		So(dtl.ElementValue(0), ShouldEqual, int8(1))
		So(dtl.Len(), ShouldEqual, 1)
		So(dtl.Value(), ShouldResemble, []interface{}{int8(1)})
		So(dtl.IsNull(0), ShouldBeFalse)

		err = dtl.SetWithRawData(0, byte(1))
		So(err, ShouldBeNil)
		So(dtl.IsNull(0), ShouldBeFalse)

		dtl.SetNull(0)
		So(dtl.IsNull(0), ShouldBeTrue)

		dtl, err = model.NewDataTypeListFromRawData(model.DtCode, []string{"code"})
		So(err, ShouldBeNil)
		So(dtl.Len(), ShouldEqual, 1)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDuration, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtComplex, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtBlob, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtChar, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDate, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDateMinute, []time.Time{date})
		So(err, ShouldBeNil)
		So(dtl.Len(), ShouldEqual, 1)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDateMinute, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDateHour, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDatetime, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDouble, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtFloat, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtInt, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtInt128, []int{0})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtIP, []int{0})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtLong, []int{0})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtMinute, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtMonth, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtNanoTime, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtSecond, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtShort, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtTime, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtTimestamp, []string{"", "invalid"})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtUUID, []int{0})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtAny, []int{0})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtString, []int{0})
		So(err, ShouldNotBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtFunction, []string{"typestr"})
		So(err, ShouldBeNil)
		So(dtl.Len(), ShouldEqual, 1)

		dtl, err = model.NewDataTypeListFromRawData(model.DtHandle, []string{"dt"})
		So(err, ShouldBeNil)
		So(dtl.Len(), ShouldEqual, 1)

		dtl.SetNull(2)

		dtl, err = model.NewDataTypeListFromRawData(model.DataTypeByte(50), []string{"dt"})
		So(err, ShouldNotBeNil)
		So(dtl, ShouldBeNil)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDuration, []int{1})
		So(err, ShouldNotBeNil)
	})
}
