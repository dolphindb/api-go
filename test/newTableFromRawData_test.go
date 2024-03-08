package test

import (
	"context"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var host13 = getRandomClusterAddress()

func TestNewTableFromRawData(t *testing.T) {
	Convey("test_NewTableFromRawData", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host13, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("test_NewTableFromRawData_with_all_datatype", func() {
			colNames := []string{"cbool", "cchar", "cshort", "cint", "clong", "cfloat", "cdouble", "cdate", "cdatetime", "cminute", "csecond", "cmonth", "cdatehour", "cnanotime", "cnanotimestamp", "ctimestamp",
				"cblob", "cdecimal32", "cdecimal64", "cstring", "csymbol", "cuuid", "cip", "cint128"}
			colTypes := []model.DataTypeByte{model.DtBool, model.DtChar, model.DtShort, model.DtInt, model.DtLong, model.DtFloat, model.DtDouble, model.DtDate, model.DtDatetime,
				model.DtMinute, model.DtSecond, model.DtMonth, model.DtDateHour, model.DtNanoTime, model.DtNanoTimestamp, model.DtTimestamp, model.DtBlob, model.DtDecimal32, model.DtDecimal64, model.DtString,
				model.DtSymbol, model.DtUUID, model.DtIP, model.DtInt128}

			cbool := []byte{1, 0, model.NullBool}
			cchar := []byte{1, 2, model.NullChar}
			cshort := []int16{1, 2, model.NullShort}
			cint := []int32{1, 2, model.NullInt}
			clong := []int64{1, 2, model.NullLong}
			cfloat := []float32{1, 2, model.NullFloat}
			cdouble := []float64{1, 2, model.NullDouble}
			cdate := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cdatetime := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cminute := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			csecond := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cmonth := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cdatehour := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cnanotime := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cnanotimestamp := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			ctimestamp := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime}
			cblob := [][]byte{[]byte("blob1"), []byte("blob2"), model.NullBlob}
			cdecimal32 := &model.Decimal32s{2, []float64{1.32244, -3.3, model.NullDecimal32Value}}
			cdecimal64 := &model.Decimal64s{11, []float64{1.32244, -3.3, model.NullDecimal64Value}}
			cstring := []string{"智臾科技", "$/-*&(!~;,'.,[]:", ""}
			csymbol := []string{"智臾科技", "$/-*&(!~;,'.,[]:", ""}
			cuuid := []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000"}
			cip := []string{"35dd:4ae6:b1b1:3da9:d777:d2ab:74cc:e05", "192.168.1.1", "0.0.0.0"}
			cint128 := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000"}

			cols := []interface{}{cbool, cchar, cshort, cint, clong, cfloat, cdouble, cdate, cdatetime, cminute, csecond, cmonth, cdatehour, cnanotime, cnanotimestamp, ctimestamp,
				cblob, cdecimal32, cdecimal64, cstring, csymbol, cuuid, cip, cint128}
			table, err := model.NewTableFromRawData(colNames, colTypes, cols)
			So(err, ShouldBeNil)
			_, err = ddb.Upload(map[string]model.DataForm{"tb": table})
			So(err, ShouldBeNil)
			res, _ := ddb.RunScript(`cbool=bool([1,0,NULL]);
									cchar=char([1,2,NULL]);
									cshort=short([1,2,NULL]);
									cint=int([1,2,NULL]);
									clong=long([1,2,NULL]);
									cfloat=float([1,2,NULL]);
									cdouble=double([1,2,NULL]);
									cdate=date([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cdatetime=datetime([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cminute=minute([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									csecond=second([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cmonth=month([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cdatehour=datehour([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cnanotime=nanotime([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cnanotimestamp=nanotimestamp([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									ctimestamp=timestamp([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cblob=blob(["blob1","blob2",""]);
									cdecimal32=decimal32([1.32244, -3.3,NULL],2);
									cdecimal64=decimal64([1.32244, -3.3,NULL],11);
									cstring=string(["智臾科技", "$/-*&(!~;,'.,[]:", ""]);
									csymbol=symbol(["智臾科技", "$/-*&(!~;,'.,[]:", ""]);
									cuuid=uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000"]);
									cip=ipaddr(["35dd:4ae6:b1b1:3da9:d777:d2ab:74cc:e05", "192.168.1.1", "0.0.0.0"]);
									cint128=int128(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000"]);
									pt=table(cbool,cchar, cshort, cint, clong, cfloat, cdouble, cdate, cdatetime, cminute, csecond, cmonth, cdatehour, cnanotime, cnanotimestamp, ctimestamp, cblob, cdecimal32, cdecimal64, cstring, csymbol, cuuid, cip, cint128);
									eqObj(pt.values(), tb.values())`)
			So(res.(*model.Scalar).Value(), ShouldBeTrue)
		})
	})
	Convey("test_NewTableFromRawData_parameter", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host13, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_ = ddb
		Convey("test_NewTableFromRawData_with_diff_colName_nums", func() {
			colNames := []string{"cbool", "cchar", "errcol"}
			colTypes := []model.DataTypeByte{model.DtBool, model.DtChar}

			cbool := []byte{1, 0, model.NullBool}
			cchar := []byte{1, 2, model.NullChar}
			cols := []interface{}{cbool, cchar}
			_, err := model.NewTableFromRawData(colNames, colTypes, cols)
			So(err.Error(), ShouldEqual, "The length of colNames, colTypes and colValues should be equal.")
		})
		Convey("test_NewTableFromRawData_with_diff_colType_nums", func() {
			colNames := []string{"cbool", "cchar"}
			colTypes := []model.DataTypeByte{model.DtBool, model.DtChar, model.DtAny}

			cbool := []byte{1, 0, model.NullBool}
			cchar := []byte{1, 2, model.NullChar}
			cols := []interface{}{cbool, cchar}
			_, err := model.NewTableFromRawData(colNames, colTypes, cols)
			So(err.Error(), ShouldEqual, "The length of colNames, colTypes and colValues should be equal.")
		})
		Convey("test_NewTableFromRawData_with_diff_col_nums", func() {
			colNames := []string{"cbool", "cchar"}
			colTypes := []model.DataTypeByte{model.DtBool, model.DtChar}

			cbool := []byte{1, 0, model.NullBool}
			cchar := []byte{1, 2, model.NullChar}
			cols := []interface{}{cbool, cchar, cbool}
			_, err := model.NewTableFromRawData(colNames, colTypes, cols)
			So(err.Error(), ShouldEqual, "The length of colNames, colTypes and colValues should be equal.")
		})
		Convey("test_NewTableFromRawData_with_colType_nil", func() {
			colNames := []string{"cbool", "cchar"}
			// colTypes := []model.DataTypeByte{model.DtBool, model.DtChar}

			cbool := []byte{1, 0, model.NullBool}
			cchar := []byte{1, 2, model.NullChar}
			cols := []interface{}{cbool, cchar}
			_, err := model.NewTableFromRawData(colNames, nil, cols)
			So(err.Error(), ShouldEqual, "The length of colNames, colTypes and colValues should be equal.")
		})
		Convey("test_NewTableFromRawData_with_colName_nil", func() {
			// colNames := []string{"cbool", "cchar"}
			colTypes := []model.DataTypeByte{model.DtBool, model.DtChar}

			cbool := []byte{1, 0, model.NullBool}
			cchar := []byte{1, 2, model.NullChar}
			cols := []interface{}{cbool, cchar}
			_, err := model.NewTableFromRawData(nil, colTypes, cols)
			So(err.Error(), ShouldEqual, "The length of colNames, colTypes and colValues should be equal.")
		})
		Convey("test_NewTableFromRawData_with_col_nil", func() {
			colNames := []string{"cbool", "cchar"}
			colTypes := []model.DataTypeByte{model.DtBool, model.DtChar}

			// cbool := []byte{1, 0, model.NullBool}
			// cchar := []byte{1, 2, model.NullChar}
			// cols := []interface{}{cbool, cchar}
			_, err := model.NewTableFromRawData(colNames, colTypes, nil)
			So(err.Error(), ShouldEqual, "The length of colNames, colTypes and colValues should be equal.")
		})

	})
}
