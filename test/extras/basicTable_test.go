package test

import (
	"bytes"
	"testing"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Table(t *testing.T) {
	Convey("Test_table", t, func() {
		dtStr, err := model.NewDataTypeListFromRawData(model.DtString, []string{"str1", "str2"})
		So(err, ShouldBeNil)

		vct := model.NewVector(dtStr)

		tb := model.NewTable([]string{"col1", "col2"}, []*model.Vector{vct})
		So(tb, ShouldBeNil)

		tb = model.NewTable([]string{}, []*model.Vector{})
		So(tb.Rows(), ShouldEqual, 0)

		colNames := []string{"void", "bool", "char", "short", "float", "double", "duration", "int", "date", "month", "time", "minute", "second",
			"datetime", "datehour", "dateminute", "long", "timestamp", "nanotime", "nanotimestamp", "int128", "ip", "uuid", "complex", "point", "string", "code",
			"function", "handle", "symbol", "blob", "any"}

		date := time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)
		dtl := model.NewEmptyDataTypeList(model.DtVoid, 1)
		void := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtBool, []bool{true})
		So(err, ShouldBeNil)
		bo := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtChar, []byte{1})
		So(err, ShouldBeNil)
		char := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtShort, []int16{1})
		So(err, ShouldBeNil)
		sh := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtFloat, []float32{1})
		So(err, ShouldBeNil)
		fl := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDouble, []float64{1})
		So(err, ShouldBeNil)
		dou := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDuration, []string{"10H"})
		So(err, ShouldBeNil)
		du := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtInt, []int32{1})
		So(err, ShouldBeNil)
		i := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDate, []time.Time{date})
		So(err, ShouldBeNil)
		da := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{date})
		So(err, ShouldBeNil)
		mo := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtTime, []time.Time{date})
		So(err, ShouldBeNil)
		ti := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{date})
		So(err, ShouldBeNil)
		mi := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{date})
		So(err, ShouldBeNil)
		sec := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{date})
		So(err, ShouldBeNil)
		dat := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{date})
		So(err, ShouldBeNil)
		dh := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtDateMinute, []time.Time{date})
		So(err, ShouldBeNil)
		dm := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtLong, []int64{1})
		So(err, ShouldBeNil)
		long := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{date})
		So(err, ShouldBeNil)
		ts := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{date})
		So(err, ShouldBeNil)
		nt := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{date})
		So(err, ShouldBeNil)
		nts := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32"})
		So(err, ShouldBeNil)
		int128 := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtIP, []string{"127.0.0.1"})
		So(err, ShouldBeNil)
		ip := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87"})
		So(err, ShouldBeNil)
		uuid := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtComplex, [][2]float64{{1, 1}})
		So(err, ShouldBeNil)
		complex := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtPoint, [][2]float64{{1, 1}})
		So(err, ShouldBeNil)
		po := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtString, []string{"10H"})
		So(err, ShouldBeNil)
		str := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtCode, []string{"10H"})
		So(err, ShouldBeNil)
		code := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtFunction, []string{"10H"})
		So(err, ShouldBeNil)
		fc := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtHandle, []string{"10H"})
		So(err, ShouldBeNil)
		hd := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtSymbol, []string{"10H"})
		So(err, ShouldBeNil)
		sym := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtBlob, [][]byte{{1, 1}})
		So(err, ShouldBeNil)
		blob := model.NewVector(dtl)

		dtl, err = model.NewDataTypeListFromRawData(model.DtAny, []model.DataForm{model.NullAny})
		So(err, ShouldBeNil)
		any := model.NewVector(dtl)
		colValues := []*model.Vector{void, bo, char, sh, fl, dou, du, i, da, mo, ti, mi, sec, dat, dh, dm, long, ts, nt, nts, int128, ip, uuid, complex, po, str, code, fc, hd, sym, blob, any}

		tb = model.NewTable(colNames, colValues)
		So(tb.Columns(), ShouldEqual, 32)

		buf := bytes.NewBuffer(nil)
		wr := protocol.NewWriter(buf)
		err = tb.Render(wr, protocol.LittleEndian)
		So(err, ShouldBeNil)
		wr.Flush()

		rd := protocol.NewReader(buf)
		df, err := model.ParseDataForm(rd, protocol.LittleEndian)
		So(err, ShouldBeNil)
		So(df.GetDataForm(), ShouldEqual, model.DfTable)
		So(df.(*model.Table).Columns(), ShouldEqual, 32)

		err = tb.Render(wr, protocol.BigEndian)
		So(err, ShouldBeNil)
		wr.Flush()

		df, err = model.ParseDataForm(rd, protocol.BigEndian)
		So(err, ShouldBeNil)
		So(df.GetDataForm(), ShouldEqual, model.DfTable)
		So(df.(*model.Table).Columns(), ShouldEqual, 32)
	})
}
