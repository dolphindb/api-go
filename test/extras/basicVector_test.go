package test

import (
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Vector(t *testing.T) {
	Convey("Test_vector", t, func() {
		Convey("Test_array_vector", func() {
			vct := model.NewVectorWithArrayVector(nil)
			So(vct, ShouldBeNil)

			dtStr, err := model.NewDataTypeListFromRawData(model.DtString, []string{"str1", "str2"})
			So(err, ShouldBeNil)

			av := model.NewArrayVector([]*model.Vector{model.NewVector(dtStr)})
			vct = model.NewVectorWithArrayVector(av)
			So(vct, ShouldBeNil)

			dtSym, err := model.NewDataTypeListFromRawData(model.DtSymbol, []string{"str1", "str2"})
			So(err, ShouldBeNil)

			av = model.NewArrayVector([]*model.Vector{model.NewVector(dtSym)})
			vct = model.NewVectorWithArrayVector(av)
			So(vct, ShouldBeNil)
		})

		Convey("test vector get set", func() {
			dtStr, err := model.NewDataTypeListFromRawData(model.DtString, []string{"str1", "str2"})
			So(err, ShouldBeNil)

			b1, err := model.NewDataType(model.DtString, "str1")
			So(err, ShouldBeNil)

			b2, err := model.NewDataType(model.DtString, "str2")
			So(err, ShouldBeNil)
			dtInt, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{0, 1})
			So(err, ShouldBeNil)
			vct := model.NewVector(dtStr)
			vct.Data = dtInt
			vct.Extend = &model.DataTypeExtend{
				BaseSize: 2,
				Base:     dtStr,
			}

			err = vct.Set(0, b2)
			So(err, ShouldBeNil)

			err = vct.Set(1, b1)
			So(err, ShouldBeNil)
			So(vct.String(), ShouldEqual, "vector<string>([str2, str1])")

			dt := vct.Get(10)
			So(dt, ShouldBeNil)

			vct.Data = nil
			vct.Extend = nil
			dt = vct.Get(0)
			So(dt, ShouldBeNil)
		})
		Convey("test vector combine", func() {
			dtStr, err := model.NewDataTypeListFromRawData(model.DtString, []string{"str1", "str2"})
			So(err, ShouldBeNil)
			date := time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)
			dtl := model.NewEmptyDataTypeList(model.DtVoid, 1)
			void := model.NewVector(dtl)
			So(void.Rows(), ShouldEqual, 1)

			void, err = void.Combine(void)
			So(err, ShouldBeNil)
			So(void.Rows(), ShouldEqual, 2)

			vct := model.NewVector(dtStr)
			vct.Data = dtStr
			void, err = void.Combine(vct)
			So(err, ShouldNotBeNil)

			dtl, err = model.NewDataTypeListFromRawData(model.DtBool, []bool{true})
			So(err, ShouldBeNil)
			bo := model.NewVector(dtl)
			So(bo.Rows(), ShouldEqual, 1)

			bo, err = bo.Combine(bo)
			So(err, ShouldBeNil)
			So(bo.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtChar, []byte{1})
			So(err, ShouldBeNil)
			char := model.NewVector(dtl)
			So(char.Rows(), ShouldEqual, 1)

			char, err = char.Combine(char)
			So(err, ShouldBeNil)
			So(char.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtShort, []int16{1})
			So(err, ShouldBeNil)
			sh := model.NewVector(dtl)
			So(sh.Rows(), ShouldEqual, 1)

			sh, err = sh.Combine(sh)
			So(err, ShouldBeNil)
			So(sh.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtFloat, []float32{1})
			So(err, ShouldBeNil)
			fl := model.NewVector(dtl)
			So(fl.Rows(), ShouldEqual, 1)

			fl, err = fl.Combine(fl)
			So(err, ShouldBeNil)
			So(fl.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtDouble, []float64{1})
			So(err, ShouldBeNil)
			dou := model.NewVector(dtl)
			So(dou.Rows(), ShouldEqual, 1)

			dou, err = dou.Combine(dou)
			So(err, ShouldBeNil)
			So(dou.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtDuration, []string{"10H"})
			So(err, ShouldBeNil)
			du := model.NewVector(dtl)
			So(du.Rows(), ShouldEqual, 1)

			du, err = du.Combine(du)
			So(err, ShouldBeNil)
			So(du.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtInt, []int32{1})
			So(err, ShouldBeNil)
			i := model.NewVector(dtl)
			So(i.Rows(), ShouldEqual, 1)

			i, err = i.Combine(i)
			So(err, ShouldBeNil)
			So(i.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtDate, []time.Time{date})
			So(err, ShouldBeNil)
			da := model.NewVector(dtl)
			So(da.Rows(), ShouldEqual, 1)

			da, err = da.Combine(da)
			So(err, ShouldBeNil)
			So(da.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{date})
			So(err, ShouldBeNil)
			mo := model.NewVector(dtl)
			So(mo.Rows(), ShouldEqual, 1)

			mo, err = mo.Combine(mo)
			So(err, ShouldBeNil)
			So(mo.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtTime, []time.Time{date})
			So(err, ShouldBeNil)
			ti := model.NewVector(dtl)
			So(ti.Rows(), ShouldEqual, 1)

			ti, err = ti.Combine(ti)
			So(err, ShouldBeNil)
			So(ti.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{date})
			So(err, ShouldBeNil)
			mi := model.NewVector(dtl)
			So(mi.Rows(), ShouldEqual, 1)

			mi, err = mi.Combine(mi)
			So(err, ShouldBeNil)
			So(mi.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{date})
			So(err, ShouldBeNil)
			sec := model.NewVector(dtl)
			So(sec.Rows(), ShouldEqual, 1)

			sec, err = sec.Combine(sec)
			So(err, ShouldBeNil)
			So(sec.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{date})
			So(err, ShouldBeNil)
			dat := model.NewVector(dtl)
			So(dat.Rows(), ShouldEqual, 1)

			dat, err = dat.Combine(dat)
			So(err, ShouldBeNil)
			So(dat.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{date})
			So(err, ShouldBeNil)
			dh := model.NewVector(dtl)
			So(dh.Rows(), ShouldEqual, 1)

			dh, err = dh.Combine(dh)
			So(err, ShouldBeNil)
			So(dh.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtDateMinute, []time.Time{date})
			So(err, ShouldBeNil)
			dm := model.NewVector(dtl)
			So(dm.Rows(), ShouldEqual, 1)

			dm, err = dm.Combine(dm)
			So(err, ShouldBeNil)
			So(dm.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtLong, []int64{1})
			So(err, ShouldBeNil)
			long := model.NewVector(dtl)
			So(long.Rows(), ShouldEqual, 1)

			long, err = long.Combine(long)
			So(err, ShouldBeNil)
			So(long.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{date})
			So(err, ShouldBeNil)
			ts := model.NewVector(dtl)
			So(ts.Rows(), ShouldEqual, 1)

			ts, err = ts.Combine(ts)
			So(err, ShouldBeNil)
			So(ts.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{date})
			So(err, ShouldBeNil)
			nt := model.NewVector(dtl)
			So(nt.Rows(), ShouldEqual, 1)

			nt, err = nt.Combine(nt)
			So(err, ShouldBeNil)
			So(nt.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{date})
			So(err, ShouldBeNil)
			nts := model.NewVector(dtl)
			So(nts.Rows(), ShouldEqual, 1)

			nts, err = nts.Combine(nts)
			So(err, ShouldBeNil)
			So(nts.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32"})
			So(err, ShouldBeNil)
			int128 := model.NewVector(dtl)
			So(int128.Rows(), ShouldEqual, 1)

			int128, err = int128.Combine(int128)
			So(err, ShouldBeNil)
			So(int128.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtIP, []string{"127.0.0.1"})
			So(err, ShouldBeNil)
			ip := model.NewVector(dtl)
			So(ip.Rows(), ShouldEqual, 1)

			ip, err = ip.Combine(ip)
			So(err, ShouldBeNil)
			So(ip.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87"})
			So(err, ShouldBeNil)
			uuid := model.NewVector(dtl)
			So(uuid.Rows(), ShouldEqual, 1)

			uuid, err = uuid.Combine(uuid)
			So(err, ShouldBeNil)
			So(uuid.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtComplex, [][2]float64{{1, 1}})
			So(err, ShouldBeNil)
			complex := model.NewVector(dtl)
			So(complex.Rows(), ShouldEqual, 1)

			complex, err = complex.Combine(complex)
			So(err, ShouldBeNil)
			So(complex.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtPoint, [][2]float64{{1, 1}})
			So(err, ShouldBeNil)
			po := model.NewVector(dtl)
			So(po.Rows(), ShouldEqual, 1)

			po, err = po.Combine(po)
			So(err, ShouldBeNil)
			So(po.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtString, []string{"10H"})
			So(err, ShouldBeNil)
			str := model.NewVector(dtl)
			So(str.Rows(), ShouldEqual, 1)

			str, err = str.Combine(str)
			So(err, ShouldBeNil)
			So(str.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtCode, []string{"10H"})
			So(err, ShouldBeNil)
			code := model.NewVector(dtl)
			So(code.Rows(), ShouldEqual, 1)

			code, err = code.Combine(code)
			So(err, ShouldBeNil)
			So(code.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtFunction, []string{"10H"})
			So(err, ShouldBeNil)
			fc := model.NewVector(dtl)
			So(fc.Rows(), ShouldEqual, 1)

			fc, err = fc.Combine(fc)
			So(err, ShouldBeNil)
			So(fc.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtHandle, []string{"10H"})
			So(err, ShouldBeNil)
			hd := model.NewVector(dtl)
			So(hd.Rows(), ShouldEqual, 1)

			hd, err = hd.Combine(hd)
			So(err, ShouldBeNil)
			So(hd.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtSymbol, []string{"10H"})
			So(err, ShouldBeNil)
			sym := model.NewVector(dtl)
			So(sym.Rows(), ShouldEqual, 1)

			sym, err = sym.Combine(sym)
			So(err, ShouldBeNil)
			So(sym.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtBlob, [][]byte{{1, 1}})
			So(err, ShouldBeNil)
			blob := model.NewVector(dtl)
			So(blob.Rows(), ShouldEqual, 1)

			blob, err = blob.Combine(blob)
			So(err, ShouldBeNil)
			So(blob.Rows(), ShouldEqual, 2)

			dtl, err = model.NewDataTypeListFromRawData(model.DtAny, []model.DataForm{model.NullAny})
			So(err, ShouldBeNil)
			any := model.NewVector(dtl)
			So(any.Rows(), ShouldEqual, 1)

			any, err = any.Combine(any)
			So(err, ShouldBeNil)
			So(any.Rows(), ShouldEqual, 2)
		})
	})
}
