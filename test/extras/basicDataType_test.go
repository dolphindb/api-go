package test

import (
	"bytes"
	"testing"
	"time"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_DataType(t *testing.T) {
	Convey("Test_DataType", t, func() {
		buf := bytes.NewBuffer(nil)
		wr := protocol.NewWriter(buf)

		sym, err := model.NewDataType(model.DataTypeByte(145), "symbol")
		So(err, ShouldBeNil)
		So(sym.DataType(), ShouldEqual, model.DtSymbol)
		So(sym.Value(), ShouldEqual, "symbol")
		So(sym.HashBucket(10), ShouldEqual, 4)

		sym, err = model.NewDataType(model.DataTypeByte(81), "symbol")
		So(err, ShouldBeNil)
		So(sym.DataType(), ShouldEqual, model.DtSymbol)
		So(sym.IsNull(), ShouldBeFalse)

		err = sym.Render(wr, protocol.LittleEndian)
		wr.Flush()
		So(err, ShouldBeNil)
		So(buf.Bytes(), ShouldResemble, []byte{115, 121, 109, 98, 111, 108, 0})
		buf.Reset()

		sym.SetNull()
		So(sym.IsNull(), ShouldBeTrue)

		cs, err := model.NewDataType(model.DtCompress, byte(1))
		So(err, ShouldBeNil)
		So(cs.DataType(), ShouldEqual, model.DtCompress)
		So(cs.String(), ShouldEqual, "1")
		So(cs.Value(), ShouldEqual, 1)
		So(cs.IsNull(), ShouldBeFalse)

		err = cs.Render(wr, protocol.LittleEndian)
		wr.Flush()
		So(err, ShouldBeNil)
		So(buf.Bytes(), ShouldResemble, []byte{1})
		buf.Reset()

		cs.SetNull()
		So(cs.IsNull(), ShouldBeTrue)

		fc, err := model.NewDataType(model.DtFunction, "getAllDBs()")
		So(err, ShouldBeNil)
		So(fc.DataType(), ShouldEqual, model.DtFunction)
		So(fc.String(), ShouldEqual, "getAllDBs()")
		So(fc.Value(), ShouldEqual, "getAllDBs()")
		So(fc.IsNull(), ShouldBeFalse)

		err = fc.Render(wr, protocol.LittleEndian)
		wr.Flush()
		So(err, ShouldBeNil)
		So(buf.Bytes(), ShouldResemble, []byte{103, 101, 116, 65, 108, 108, 68, 66, 115, 40, 41, 0})
		buf.Reset()

		fc.SetNull()
		So(fc.IsNull(), ShouldBeTrue)

		co, err := model.NewDataType(model.DtCode, "getAllDBs()")
		So(err, ShouldBeNil)
		So(co.DataType(), ShouldEqual, model.DtCode)
		So(co.String(), ShouldEqual, "getAllDBs()")
		So(co.Value(), ShouldEqual, "getAllDBs()")
		So(co.IsNull(), ShouldBeFalse)

		co.SetNull()
		So(co.IsNull(), ShouldBeTrue)

		dl, err := model.NewDataType(model.DtHandle, "dt")
		So(err, ShouldBeNil)
		So(dl.DataType(), ShouldEqual, model.DtHandle)
		So(dl.String(), ShouldEqual, "dt")
		So(dl.IsNull(), ShouldBeFalse)

		err = dl.Render(wr, protocol.LittleEndian)
		wr.Flush()
		So(err, ShouldBeNil)
		So(buf.Bytes(), ShouldResemble, []byte{100, 116, 0})
		buf.Reset()

		dl.SetNull()
		So(dl.IsNull(), ShouldBeTrue)

		_, err = model.NewDataType(model.DataTypeByte(45), "dt")
		So(err, ShouldBeNil)

		b, err := model.NewDataType(model.DtBlob, []byte{0, 1})
		So(err, ShouldBeNil)
		So(b.IsNull(), ShouldBeFalse)
		So(b.String(), ShouldNotBeNil)

		ok, err := b.Bool()
		So(err, ShouldNotBeNil)
		So(ok, ShouldBeFalse)

		b.SetNull()
		So(b.IsNull(), ShouldBeTrue)

		s, err := model.NewDataType(model.DtShort, int16(1))
		So(err, ShouldBeNil)
		So(s.IsNull(), ShouldBeFalse)
		So(s.HashBucket(10), ShouldEqual, 1)

		s.SetNull()
		So(s.HashBucket(10), ShouldEqual, -1)

		c, err := model.NewDataType(model.DtChar, byte(1))
		So(err, ShouldBeNil)
		So(c.IsNull(), ShouldBeFalse)
		So(c.HashBucket(10), ShouldEqual, 1)

		l, err := model.NewDataType(model.DtLong, int64(1))
		So(err, ShouldBeNil)
		So(l.IsNull(), ShouldBeFalse)
		So(l.HashBucket(10), ShouldEqual, 1)

		f, err := model.NewDataType(model.DtFloat, float32(1))
		So(err, ShouldBeNil)
		So(f.IsNull(), ShouldBeFalse)
		So(f.HashBucket(10), ShouldEqual, -1)

		v, err := model.NewDataType(model.DtVoid, nil)
		So(err, ShouldBeNil)
		So(v.IsNull(), ShouldBeTrue)

		v.SetNull()
		So(v.IsNull(), ShouldBeTrue)

		v, err = model.NewDataType(model.DtVoid, model.NewScalar(v))
		So(err, ShouldBeNil)
		So(v.IsNull(), ShouldBeTrue)

		comp, err := model.NewDataType(model.DtComplex, [2]float64{1, 1})
		So(err, ShouldBeNil)
		So(comp.IsNull(), ShouldBeFalse)
		So(comp.HashBucket(10), ShouldEqual, -1)

		comp.SetNull()
		So(comp.IsNull(), ShouldBeTrue)

		po, err := model.NewDataType(model.DtPoint, [2]float64{1, 1})
		So(err, ShouldBeNil)
		So(po.IsNull(), ShouldBeFalse)

		po.SetNull()
		So(po.IsNull(), ShouldBeTrue)
		So(po.String(), ShouldEqual, "(,)")

		sca := model.NewScalar(po)
		any, err := model.NewDataType(model.DtAny, sca)
		So(err, ShouldBeNil)
		So(any.IsNull(), ShouldBeFalse)

		err = any.Render(wr, protocol.LittleEndian)
		wr.Flush()
		So(err, ShouldBeNil)
		So(buf.Bytes(), ShouldResemble, []byte{35, 0, 255, 255, 255, 255, 255, 255, 239, 255, 255, 255, 255, 255, 255, 255, 239, 255})
		buf.Reset()

		any.SetNull()
		So(any.IsNull(), ShouldBeTrue)

		ip, err := model.NewDataType(model.DtIP, "127.0.0.1")
		So(err, ShouldBeNil)
		So(ip.IsNull(), ShouldBeFalse)
		So(ip.HashBucket(10), ShouldEqual, 6)

		err = ip.Render(wr, protocol.LittleEndian)
		wr.Flush()
		So(err, ShouldBeNil)
		So(buf.Bytes(), ShouldResemble, []byte{1, 0, 0, 127, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		buf.Reset()

		ip.SetNull()
		So(ip.IsNull(), ShouldBeTrue)
		So(ip.String(), ShouldEqual, "0.0.0.0")

		int128, err := model.NewDataType(model.DtInt128, "e1671797c52e15f763380b45e841ec32")
		So(err, ShouldBeNil)
		So(int128.IsNull(), ShouldBeFalse)

		int128.SetNull()
		So(int128.IsNull(), ShouldBeTrue)
		So(int128.String(), ShouldEqual, "00000000000000000000000000000000")

		uuid, err := model.NewDataType(model.DtUUID, "5d212a78-cc48-e3b1-4235-b4d91473ee87")
		So(err, ShouldBeNil)
		So(uuid.IsNull(), ShouldBeFalse)
		So(uuid.HashBucket(10), ShouldEqual, 8)

		uuid.SetNull()
		So(uuid.IsNull(), ShouldBeTrue)
		So(uuid.String(), ShouldEqual, "00000000-0000-0000-0000-000000000000")

		du, err := model.NewDataType(model.DtDuration, "10h")
		So(err, ShouldBeNil)
		So(du.HashBucket(10), ShouldEqual, 0)

		date := time.Date(2022, 1, 1, 1, 1, 1, 1, time.UTC)
		ti, err := model.NewDataType(model.DtTime, date)
		So(err, ShouldBeNil)
		So(ti.HashBucket(10), ShouldEqual, 0)

		mo, err := model.NewDataType(model.DtMonth, date)
		So(err, ShouldBeNil)
		So(mo.HashBucket(10), ShouldEqual, 4)

		mi, err := model.NewDataType(model.DtMinute, date)
		So(err, ShouldBeNil)
		So(mi.HashBucket(10), ShouldEqual, 1)

		se, err := model.NewDataType(model.DtSecond, date)
		So(err, ShouldBeNil)
		So(se.HashBucket(10), ShouldEqual, 1)

		dh, err := model.NewDataType(model.DtDateHour, date)
		So(err, ShouldBeNil)
		So(dh.HashBucket(10), ShouldEqual, 3)

		dh.SetNull()
		So(dh.HashBucket(10), ShouldEqual, -1)

		dt, err := model.NewDataType(model.DtDatetime, date)
		So(err, ShouldBeNil)
		So(dt.HashBucket(10), ShouldEqual, 1)

		nt, err := model.NewDataType(model.DtNanoTime, date)
		So(err, ShouldBeNil)
		So(nt.HashBucket(10), ShouldEqual, 1)

		nts, err := model.NewDataType(model.DtNanoTimestamp, date)
		So(err, ShouldBeNil)
		So(nts.HashBucket(10), ShouldEqual, 1)

		ts, err := model.NewDataType(model.DtTimestamp, date)
		So(err, ShouldBeNil)
		So(ts.HashBucket(10), ShouldEqual, 0)

		dm, err := model.NewDataType(model.DtDateMinute, date)
		So(err, ShouldBeNil)
		So(dm.String(), ShouldEqual, "2022.01.01T01:01")
		So(dm.Value(), ShouldNotBeNil)

		str, err := model.NewDataType(model.DtString, string([]rune{'\u0000', '\u0002', '\u0081'}))
		So(err, ShouldBeNil)
		So(str.HashBucket(10), ShouldEqual, 4)
	})
}
