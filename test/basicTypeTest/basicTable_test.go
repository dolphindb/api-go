package test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Table_DownLoad_DataType_string(t *testing.T) {
	Convey("Test_Table_with_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_string_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("sym").Data.Value()
			zx := [6]string{"IBM", "C", "MS", "MSFT", "JPM", "ORCL"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			form := result.GetDataForm()
			So(form, ShouldEqual, 6)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 0)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "void")
			row := result.Rows()
			col := result.Columns()
			So(row, ShouldEqual, 6)
			So(col, ShouldEqual, 10)
			ids := []int{0}
			sub := result.GetSubtable(ids)
			So(sub, ShouldNotBeNil)
		})
		Convey("Test_Table_with_string_has_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C``MSFT``ORCL;m=table(2022.08.03+take(100..105,n) as date,take(syms,n) as sym, 2012.08M+take(100..105,n) as month, 09:30:00.000+take(100..105,n) as time, 09:30m+take(100..105,n) as minute );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("sym").Data.Value()
			zx := [6]string{"IBM", "C", "", "MSFT", "", "ORCL"}
			for i := 0; i < len(get); i++ {
				So(get[i], ShouldEqual, zx[i])
			}
		})
		Convey("Test_Table_only_one_string_columns:", func() {
			s, err := db.RunScript("em = `IBM`C`MS`MSFT`JPM`ORCL;m = table(take(em,6) as string);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("string").Data.Value()
			zx := [6]string{"IBM", "C", "MS", "MSFT", "JPM", "ORCL"}
			for i := 0; i < len(get); i++ {
				So(get[i], ShouldEqual, zx[i])
			}
		})
		Convey("Test_Table_only_one_string_null_columns:", func() {
			s, err := db.RunScript("em = ``````;m = table(take(em,6) as string_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("string_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("string_null").IsNull(i), ShouldEqual, true)
			}
		})
		Convey("Test_Table_with_string_all_null:", func() {
			s, err := db.RunScript("n=6;syms=``````;m=table(2022.08.03 11:00:00+take(10..15,n) as datetime,take(syms,n) as sym, 2012.08.03 11:00:00.000+take(100..105,n) as timestamp, 11:00:00.000000000+take(100..105,n) as nanotime, 2022.08.03 11:00:00.000000000+take(100..105,n) as nanotimestamp );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("sym").Data.Value()
			zx := [6]string{"", "", "", "", "", ""}
			for i := 0; i < len(get); i++ {
				So(get[i], ShouldEqual, zx[i])
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_bool(t *testing.T) {
	Convey("Test_Table_with_bool:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_bool_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("bool").Data.Value()
			zx := [6]bool{true, false, false, true, false, true}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_bool_columns:", func() {
			s, err := db.RunScript("em = true false false true false true;m = table(take(em,6) as bool);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("bool").Data.Value()
			zx := [6]bool{true, false, false, true, false, true}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_bool_null_columns:", func() {
			s, err := db.RunScript("m = table(take(00b,6) as bool_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("bool_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("bool_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_symbol(t *testing.T) {
	Convey("Test_Table_with_symbol:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_symbol_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("symbol").Data.Value()
			zx := [6]string{"A", "B", "C", "D", "E", "F"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_symbol_columns:", func() {
			s, err := db.RunScript("em = symbol(`A`B`C`D`E`F);m = table(take(em,6) as symbol);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("symbol").Data.Value()
			zx := [6]string{"A", "B", "C", "D", "E", "F"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_symbol_null_columns:", func() {
			s, err := db.RunScript("em = symbol(['','','','','','']);m = table(take(em,6) as symbol_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("symbol_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("symbol_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_double(t *testing.T) {
	Convey("Test_Table_with_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_double_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("qty").Data.Value()
			zx := [6]float64{1010, 1020, 1030, 1040, 1050, 1060}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_double_columns:", func() {
			s, err := db.RunScript("m = table(10.0*(1+take(100..105,6)) as double);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("double").Data.Value()
			zx := [6]float64{1010, 1020, 1030, 1040, 1050, 1060}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_double_null_columns:", func() {
			s, err := db.RunScript("m = table(10.0+double(['','','','','','']) as double_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("double_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("double_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_int(t *testing.T) {
	Convey("Test_Table_with_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_int_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("price").Data.Value()
			zx := [6]int32{105, 106, 107, 108, 109, 110}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_int_columns:", func() {
			s, err := db.RunScript("m = table(10+take(10..15,6) as int);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("int").Data.Value()
			zx := [6]int32{20, 21, 22, 23, 24, 25}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_int_null_columns:", func() {
			s, err := db.RunScript("m = table(10+take(00i,6) as int_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("int_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("int_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_float(t *testing.T) {
	Convey("Test_Table_with_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_float_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("float").Data.Value()
			zx := [6]float32{606, 612, 618, 624, 630, 636}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_float_columns:", func() {
			s, err := db.RunScript("m = table(10f*(1+take(10..15,6)) as float);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("float").Data.Value()
			zx := [6]float32{110, 120, 130, 140, 150, 160}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_float_null_columns:", func() {
			s, err := db.RunScript("m = table(10+take(00f,6) as float_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("float_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("float_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_long(t *testing.T) {
	Convey("Test_Table_with_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_long_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("long").Data.Value()
			zx := [6]int64{505, 510, 515, 520, 525, 530}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_long_columns:", func() {
			s, err := db.RunScript("m = table(10l+take(10..15,6) as long);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("long").Data.Value()
			zx := [6]int64{20, 21, 22, 23, 24, 25}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_long_null_columns:", func() {
			s, err := db.RunScript("m = table(10+take(00l,6) as long_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("long_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("long_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_char(t *testing.T) {
	Convey("Test_Table_with_char:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_char_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("char").Data.Value()
			zx := [6]uint8{97, 102, 97, 100, 99, 98}
			for i := 0; i < len(get); i++ {
				So(get[i], ShouldEqual, zx[i])
			}
		})
		Convey("Test_Table_only_one_char_columns:", func() {
			s, err := db.RunScript("em = 97c 102c 97c 100c 99c 98c;m = table(take(em,6) as char);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("char").Data.Value()
			zx := [6]uint{97, 102, 97, 100, 99, 98}
			for i := 0; i < len(get); i++ {
				So(get[i], ShouldEqual, zx[i])
			}
		})
		Convey("Test_Table_only_one_char_null_columns:", func() {
			s, err := db.RunScript("em = char(['','','','','','']);m = table(take(em,6) as char_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("char_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("char_null").IsNull(i), ShouldEqual, true)
			}
			So(result.String(), ShouldNotBeNil)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_short(t *testing.T) {
	Convey("Test_Table_with_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_short_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(09:30:00+take(100..105,n) as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("short").Data.Value()
			zx := [6]int16{11, 12, 13, 14, 15, 16}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_short_columns:", func() {
			s, err := db.RunScript("sh = 11h 12h 13h 14h 15h 16h;m = table(take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("short").Data.Value()
			zx := [6]int16{11, 12, 13, 14, 15, 16}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_short_null_columns:", func() {
			s, err := db.RunScript("sh = short(['','','','','','']);m = table(take(sh,6) as short_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("short_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("short_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_date(t *testing.T) {
	Convey("Test_Table_with_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_date_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C``MSFT``ORCL;m=table(1969.12.31 1970.01.01 1970.01.02 2006.01.02 2006.01.03 2022.08.03 as date,take(syms,n) as sym, 2012.08M+take(100..105,n) as month, 09:30:00.000+take(100..105,n) as time, 09:30m+take(100..105,n) as minute );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("date").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_date_columns:", func() {
			s, err := db.RunScript("m = table(2022.08.03+take(100..105,6) as date);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("date").Data.Value()
			time1 := []time.Time{time.Date(2022, 11, 11, 0, 0, 0, 0, time.UTC), time.Date(2022, 11, 12, 0, 0, 0, 0, time.UTC), time.Date(2022, 11, 13, 0, 0, 0, 0, time.UTC), time.Date(2022, 11, 14, 0, 0, 0, 0, time.UTC), time.Date(2022, 11, 15, 0, 0, 0, 0, time.UTC), time.Date(2022, 11, 16, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_date_null_columns:", func() {
			s, err := db.RunScript("m = table(10+take(00d,6) as date_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("date_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("date_null").IsNull(i), ShouldEqual, true)
			}
			idex := result.GetColumnByIndex(15)
			So(idex, ShouldBeNil)
			byna := result.GetColumnByName("hello")
			So(byna, ShouldBeNil)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_month(t *testing.T) {
	Convey("Test_Table_with_month:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_month_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C``MSFT``ORCL;m=table(2022.08.03+take(100..105,n) as date,take(syms,n) as sym, 1969.12M 1970.01M 1970.02M 2006.01M 2006.02M 2022.08M as month, 09:30:00.000+take(100..105,n) as time, 09:30m+take(100..105,n) as minute );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("month").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 1, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_month_columns:", func() {
			s, err := db.RunScript("n=6;m = table(2012.08M+take(100..105,n) as month);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("month").Data.Value()
			time1 := []time.Time{time.Date(2020, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 4, 1, 0, 0, 0, 0, time.UTC), time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_month_null_columns:", func() {
			s, err := db.RunScript("m = table(10+month(['','','','','','']) as month_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("month_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("month_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_time(t *testing.T) {
	Convey("Test_Table_with_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_time_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C``MSFT``ORCL;m=table(2022.08.03+take(100..105,n) as date,take(syms,n) as sym, 2012.08M+take(100..105,n) as month, 23:59:59.999 00:00:00.000 00:00:01.999 15:04:04.999 15:04:05.000 15:00:15.000 as time, 09:30m+take(100..105,n) as minute );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("time").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_time_columns:", func() {
			s, err := db.RunScript("n=6;m = table(23:59:59.999 00:00:00.000 00:00:01.999 15:04:04.999 15:04:05.000 15:00:15.000 as time);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("time").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_time_null_columns:", func() {
			s, err := db.RunScript("m = table(10+time(['','','','','','']) as time_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("time_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("time_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_minute(t *testing.T) {
	Convey("Test_Table_with_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_minute_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C``MSFT``ORCL;m=table(2022.08.03+take(100..105,n) as date,take(syms,n) as sym, 2012.08M+take(100..105,n) as month, 09:30:00.000+take(100..105,n) as time, 23:59m 00:00m 00:01m 15:04m 15:05m 15:15m as minute );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("minute").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 1, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 5, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 15, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_minute_columns:", func() {
			s, err := db.RunScript("n=6; m = table(23:59m 00:00m 00:01m 15:04m 15:05m 15:15m as minute );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("minute").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 1, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 5, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 15, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_minute_null_columns:", func() {
			s, err := db.RunScript("m = table(10+take(00m,6) as minute_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("minute_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("minute_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_second(t *testing.T) {
	Convey("Test_Table_with_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_second_not_null:", func() {
			s, err := db.RunScript("n=6;syms=`IBM`C`MS`MSFT`JPM`ORCL;em = 97c 102c 97c 100c 99c 98c;zx=true false false true false true;ax = symbol(`A`B`C`D`E`F);sh = 11h 12h 13h 14h 15h 16h;m=table(23:59:59 00:00:00 00:00:01 15:04:04 15:04:05 15:00:15 as second,take(syms,n) as sym, 10.0*(1+take(100..105,n)) as qty,5.0+take(100..105,n) as price, 5l*(1+take(100..105,n)) as long, 6f*(1+take(100..105,n)) as float,take(em,n) as char,take(zx,n) as bool, take(ax,n) as symbol,take(sh,6) as short);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("second").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_second_columns:", func() {
			s, err := db.RunScript("m = table(23:59:59 00:00:00 00:00:01 15:04:04 15:04:05 15:00:15 as second );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("second").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_second_null_columns:", func() {
			s, err := db.RunScript("m = table(10+take(00s,6) as second_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("second_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("second_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_datetime(t *testing.T) {
	Convey("Test_Table_with_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_datetime_not_null:", func() {
			s, err := db.RunScript("n=6;syms=``````;m=table(1969.12.31T23:59:59 1970.01.01T00:00:00 1970.01.01T00:00:01 2006.01.02T15:04:04 2006.01.02T15:04:05 2022.08.03T15:00:15 as datetime,take(syms,n) as sym, 2012.08.03 11:00:00.000+take(100..105,n) as timestamp, 11:00:00.000000000+take(100..105,n) as nanotime, 2022.08.03 11:00:00.000000000+take(100..105,n) as nanotimestamp );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("datetime").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_datetime_columns:", func() {
			s, err := db.RunScript("m = table(1969.12.31T23:59:59 1970.01.01T00:00:00 1970.01.01T00:00:01 2006.01.02T15:04:04 2006.01.02T15:04:05 2022.08.03T15:00:15 as  datetime);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("datetime").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_datetime_null_columns:", func() {
			s, err := db.RunScript("m = table(10+datetime(['','','','','','']) as datetime_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("datetime_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("datetime_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_timestamp(t *testing.T) {
	Convey("Test_Table_with_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_timestamp_not_null:", func() {
			s, err := db.RunScript("n=6;syms=``````;m=table(2022.08.03 11:00:00+take(10..15,n) as datetime,take(syms,n) as sym, 1969.12.31T23:59:59.999 1970.01.01T00:00:00.000 1970.01.01T00:00:01.999 2006.01.02T15:04:04.999 2006.01.02T15:04:05.000 2022.08.03T15:00:15.000 as timestamp, 11:00:00.000000000+take(100..105,n) as nanotime, 2022.08.03 11:00:00.000000000+take(100..105,n) as nanotimestamp );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("timestamp").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_timestamp_columns:", func() {
			s, err := db.RunScript("m = table(1969.12.31T23:59:59.999 1970.01.01T00:00:00.000 1970.01.01T00:00:01.999 2006.01.02T15:04:04.999 2006.01.02T15:04:05.000 2022.08.03T15:00:15.000 as timestamp);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("timestamp").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_timestamp_null_columns:", func() {
			s, err := db.RunScript("m = table(10+timestamp(['','','','','','']) as timestamp_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("timestamp_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("timestamp_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_nanotime(t *testing.T) {
	Convey("Test_Table_with_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_nanotime_not_null:", func() {
			s, err := db.RunScript("n=6;syms=``````;m=table(2022.08.03 11:00:00+take(10..15,n) as datetime,take(syms,n) as sym, 2012.08.03 11:00:00.000+take(100..105,n) as timestamp, 23:59:59.999999999 00:00:00.000000000 00:00:01.999999999 15:04:04.999999999 15:04:05.000000000 15:00:15.000000000 as nanotime, 2022.08.03 11:00:00.000000000+take(100..105,n) as nanotimestamp );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("nanotime").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_nanotime_columns:", func() {
			s, err := db.RunScript("m = table(23:59:59.999999999 00:00:00.000000000 00:00:01.999999999 15:04:04.999999999 15:04:05.000000000 15:00:15.000000000 as nanotime);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("nanotime").Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_nanotime_null_columns:", func() {
			s, err := db.RunScript("m = table(10+nanotime(['','','','','','']) as nanotime_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("nanotime_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("nanotime_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_nanotimestamp(t *testing.T) {
	Convey("Test_Table_with_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_nannotimestamp_not_null:", func() {
			s, err := db.RunScript("n=6;syms=``````;m=table(2022.08.03 11:00:00+take(10..15,n) as datetime,take(syms,n) as sym, 2012.08.03 11:00:00.000+take(100..105,n) as timestamp, 11:00:00.000000000+take(100..105,n) as nanotime, 1969.12.31T23:59:59.999999999 1970.01.01T00:00:00.000000000 1970.01.01T00:00:01.999999999 2006.01.02T15:04:04.999999999 2006.01.02T15:04:05.000000000 2022.08.03T15:00:15.000000000 as nanotimestamp );m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("nanotimestamp").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_nanotimestamp_columns:", func() {
			s, err := db.RunScript("m = table(1969.12.31T23:59:59.999999999 1970.01.01T00:00:00.000000000 1970.01.01T00:00:01.999999999 2006.01.02T15:04:04.999999999 2006.01.02T15:04:05.000000000 2022.08.03T15:00:15.000000000 as nanotimestamp);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("nanotimestamp").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_nanotimestamp_null_columns:", func() {
			s, err := db.RunScript("m = table(10+nanotimestamp(['','','','','','']) as nanotimestamp_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("nanotimestamp_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("nanotimestamp_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_datehour(t *testing.T) {
	Convey("Test_Table_with_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_datehour_not_null:", func() {
			s, err := db.RunScript("ax = uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89']);bx =datehour[1969.12.31T23:59:59.999, 1970.01.01T00:00:00.000,  2006.01.02T15:04:04.999];cx = ipaddr(['461c:7fa1:7f3c:7249:5278:c610:f595:d174', '3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72', '127e:eeed:1b16:20a9:1694:6185:f045:fb9a']);dx = ipaddr(['192.168.1.135', '192.168.1.124', '192.168.1.14']);zx = int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34']); m = table(take(ax,3) as uuid, take(bx,3) as datehour,take(cx,3) as ipaddr,take(dx,3) as ipaddr123,take(zx,3) as int128);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("datehour").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_datehour_columns:", func() {
			s, err := db.RunScript("bx =datehour[1969.12.31T23:59:59.999, 1970.01.01T00:00:00.000,  2006.01.02T15:04:04.999];m = table(take(bx,3) as datehour);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("datehour").Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_datehour_null_columns:", func() {
			s, err := db.RunScript("m = table(10.0+datehour(['','','','','','']) as datehour_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("datehour_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("datehour_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_uuid(t *testing.T) {
	Convey("Test_Table_with_uuid:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_uuid_not_null:", func() {
			s, err := db.RunScript("ax = uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89']);bx =datehour([2022.07.29 15:00:00.000, 2022.07.29 16:00:00.000, 2022.07.29 17:00:00.000]);cx = ipaddr(['461c:7fa1:7f3c:7249:5278:c610:f595:d174', '3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72', '127e:eeed:1b16:20a9:1694:6185:f045:fb9a']);dx = ipaddr(['192.168.1.135', '192.168.1.124', '192.168.1.14']);zx = int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34']); m = table(take(ax,3) as uuid, take(bx,3) as datehour,take(cx,3) as ipaddr,take(dx,3) as ipaddr123,take(zx,3) as int128);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("uuid").Data.Value()
			zx := [3]string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_uuid_columns:", func() {
			s, err := db.RunScript("ax = uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89']);m = table(take(ax,3) as uuid);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("uuid").Data.Value()
			zx := [3]string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_uuid_null_columns:", func() {
			s, err := db.RunScript("m = table(uuid(['','','','','','']) as uuid_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("uuid_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("uuid_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_ipaddr(t *testing.T) {
	Convey("Test_Table_with_ipaddr:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_ipaddr_not_null:", func() {
			s, err := db.RunScript("ax = uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89']);bx =datehour([2022.07.29 15:00:00.000, 2022.07.29 16:00:00.000, 2022.07.29 17:00:00.000]);cx = ipaddr(['461c:7fa1:7f3c:7249:5278:c610:f595:d174', '3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72', '127e:eeed:1b16:20a9:1694:6185:f045:fb9a']);dx = ipaddr(['192.168.1.135', '192.168.1.124', '192.168.1.14']);zx = int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34']); m = table(take(ax,3) as uuid, take(bx,3) as datehour,take(cx,3) as ipaddr,take(dx,3) as ipaddr123,take(zx,3) as int128);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("ipaddr").Data.Value()
			zx := [3]string{"461c:7fa1:7f3c:7249:5278:c610:f595:d174", "3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72", "127e:eeed:1b16:20a9:1694:6185:f045:fb9a"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			Convey("Test_Table_ipaddr_num: ", func() {
				get := result.GetColumnByName("ipaddr123").Data.Value()
				zx := [3]string{"192.168.1.135", "192.168.1.124", "192.168.1.14"}
				var k int
				for i := 0; i < len(get); i++ {
					if get[i] == zx[i] {
						k++
					}
				}
				So(k, ShouldEqual, result.Rows())
			})
			Convey("Test_Table_only_one_ipaddr_columns:", func() {
				s, err := db.RunScript("cx = ipaddr(['461c:7fa1:7f3c:7249:5278:c610:f595:d174', '3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72', '127e:eeed:1b16:20a9:1694:6185:f045:fb9a']);m = table(take(cx,3) as ipaddr);m")
				So(err, ShouldBeNil)
				result := s.(*model.Table)
				get := result.GetColumnByName("ipaddr").Data.Value()
				zx := [3]string{"461c:7fa1:7f3c:7249:5278:c610:f595:d174", "3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72", "127e:eeed:1b16:20a9:1694:6185:f045:fb9a"}
				var k int
				for i := 0; i < len(get); i++ {
					if get[i] == zx[i] {
						k++
					}
				}
				So(k, ShouldEqual, result.Rows())
			})
			Convey("Test_Table_only_one_ipaddr_null_columns:", func() {
				s, err := db.RunScript("m = table(ipaddr(['','','','','','']) as ipaddr_null);m")
				So(err, ShouldBeNil)
				result := s.(*model.Table)
				get := result.GetColumnByName("ipaddr_null").Data.Value()
				for i := 0; i < len(get); i++ {
					So(result.GetColumnByName("ipaddr_null").IsNull(i), ShouldBeTrue)
				}
			})
			So(db.Close(), ShouldBeNil)
		})
	})
}
func Test_Table_DownLoad_DataType_int128(t *testing.T) {
	Convey("Test_Table_with_int128:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_int128_not_null:", func() {
			s, err := db.RunScript("ax = uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89']);bx =datehour([2022.07.29 15:00:00.000, 2022.07.29 16:00:00.000, 2022.07.29 17:00:00.000]);cx = ipaddr(['461c:7fa1:7f3c:7249:5278:c610:f595:d174', '3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72', '127e:eeed:1b16:20a9:1694:6185:f045:fb9a']);dx = ipaddr(['192.168.1.135', '192.168.1.124', '192.168.1.14']);zx = int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34']); m = table(take(ax,3) as uuid, take(bx,3) as datehour,take(cx,3) as ipaddr,take(dx,3) as ipaddr123,take(zx,3) as int128);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("int128").Data.Value()
			zx := [3]string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_int128_columns:", func() {
			s, err := db.RunScript("zx = int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34']);m = table(take(zx,3) as int128);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("int128").Data.Value()
			zx := [3]string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"}
			var k int
			for i := 0; i < len(get); i++ {
				if get[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
		})
		Convey("Test_Table_only_one_int128_null_columns:", func() {
			s, err := db.RunScript("m = table(10.0+int128(['','','','','','']) as int128_null);m")
			So(err, ShouldBeNil)
			result := s.(*model.Table)
			get := result.GetColumnByName("int128_null").Data.Value()
			for i := 0; i < len(get); i++ {
				So(result.GetColumnByName("int128_null").IsNull(i), ShouldEqual, true)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_big_size(t *testing.T) {
	Convey("Test_Table_size_bigger_than_1024:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("table1 = table(1..2048 as id, take(symbol((`A)+string(1..10)),2048) as name, double(rand(3892,2048)) as value);select * from table1")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 2048)
		So(db.Close(), ShouldBeNil)
	})
	Convey("Test_Table_size_bigger_than_1048576:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n = 2*1048576; table1 = table(1..n as id, take(symbol((`A)+string(1..10)),n) as name, double(rand(3892,n)) as value);select * from table1")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 2097152)
		So(db.Close(), ShouldBeNil)
	})
	Convey("Test_Table_size_bigger_than_3000000:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n = 4000000; table1 = table(1..n as id, take(symbol((`A)+string(1..10)),n) as name, double(rand(3892,n)) as value);select * from table1")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 4000000)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_zero_row(t *testing.T) {
	Convey("Test_Table_with_one_row:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("create table table1(id INT,name SYMBOL,value DOUBLE);go;select * from table1")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 0)
		So(result.GetColumnByName("id").IsNull(0), ShouldBeTrue)
		So(result.GetColumnByName("name").IsNull(0), ShouldBeTrue)
		So(result.GetColumnByName("value").IsNull(0), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_DataType_one_row(t *testing.T) {
	Convey("Test_Table_with_one_row:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("create table table1(id INT,name SYMBOL,value DOUBLE);go;insert into table1 values(1,`A,12.4);select * from table1")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 1)
		id := result.GetColumnByName("id").Data.Value()
		name := result.GetColumnByName("name").Data.Value()
		value := result.GetColumnByName("value").Data.Value()
		So(id[0], ShouldEqual, 1)
		So(name[0], ShouldEqual, "A")
		So(value[0], ShouldEqual, 12.4)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_Distributed_table(t *testing.T) {
	Convey("Test_Table_distributed_table:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("dbName = 'dfs://Valuedb';if(existsDatabase(dbName)){dropDatabase(dbName)};n = 6;datetimev = take(2022.01.03T12:00:00+1..n, n);num = take(0.0+1..n, n);name = take(`a`b`c, n);boolv = take(true false true, n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t=table(datetimev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);db=database('dfs://Valuedb', VALUE,  `a`b`c);pt=db.createPartitionedTable(t, `pt, `name).append!(t);select * from pt;")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1, 4, 2, 5, 3, 6}
		name := []string{"a", "a", "b", "b", "c", "c"}
		boolv := []bool{true, true, false, false, true, true}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(4.00000, 104.00000)", "(2.00000, 102.00000)", "(5.00000, 105.00000)", "(3.00000, 103.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "4.00000+104.00000i", "2.00000+102.00000i", "5.00000+105.00000i", "3.00000+103.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_Dimension_table(t *testing.T) {
	Convey("Test_Table_dimension_table:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("dbName = 'dfs://db1';if(existsDatabase(dbName)){dropDatabase(dbName)};db=database('dfs://db1',VALUE,1 2 3);n = 6;timev = take(2022.01.03T12:00:00+1..n, n);num = take(1023.002+1..n, n);name = take(`a`b`c`d`e`f, n);boolv = take(true false , n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t=table(timev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);dt=db.createTable(t,`dt).append!(t);select * from dt")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1024.002, 1025.002, 1026.002, 1027.002, 1028.002, 1029.002}
		name := []string{"a", "b", "c", "d", "e", "f"}
		boolv := []bool{true, false, true, false, true, false}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(2.00000, 102.00000)", "(3.00000, 103.00000)", "(4.00000, 104.00000)", "(5.00000, 105.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "2.00000+102.00000i", "3.00000+103.00000i", "4.00000+104.00000i", "5.00000+105.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_StreamData_table(t *testing.T) {
	Convey("Test_Table_streamData_table:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n = 6;timev = take(2022.01.03T12:00:00+1..n, n);num = take(1023.002+1..n, n);name = take(`a`b`c`d`e`f, n);boolv = take(true false , n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t=streamTable(timev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);select * from t")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1024.002, 1025.002, 1026.002, 1027.002, 1028.002, 1029.002}
		name := []string{"a", "b", "c", "d", "e", "f"}
		boolv := []bool{true, false, true, false, true, false}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(2.00000, 102.00000)", "(3.00000, 103.00000)", "(4.00000, 104.00000)", "(5.00000, 105.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "2.00000+102.00000i", "3.00000+103.00000i", "4.00000+104.00000i", "5.00000+105.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_indexed_table(t *testing.T) {
	Convey("Test_Table_indexed_table:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n = 6;timev = take(2022.01.03T12:00:00+1..n, n);num = take(1023.002+1..n, n);name = take(`a`b`c`d`e`f, n);boolv = take(true false , n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t1=table(timev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);t=indexedTable(`timev, t1);select * from t")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1024.002, 1025.002, 1026.002, 1027.002, 1028.002, 1029.002}
		name := []string{"a", "b", "c", "d", "e", "f"}
		boolv := []bool{true, false, true, false, true, false}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(2.00000, 102.00000)", "(3.00000, 103.00000)", "(4.00000, 104.00000)", "(5.00000, 105.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "2.00000+102.00000i", "3.00000+103.00000i", "4.00000+104.00000i", "5.00000+105.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_keyed_table(t *testing.T) {
	Convey("Test_Table_keyed_table:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n = 6;timev = take(2022.01.03T12:00:00+1..n, n);num = take(1023.002+1..n, n);name = take(`a`b`c`d`e`f, n);boolv = take(true false , n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t1=table(timev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);t=keyedTable(`timev, t1);select * from t")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1024.002, 1025.002, 1026.002, 1027.002, 1028.002, 1029.002}
		name := []string{"a", "b", "c", "d", "e", "f"}
		boolv := []bool{true, false, true, false, true, false}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(2.00000, 102.00000)", "(3.00000, 103.00000)", "(4.00000, 104.00000)", "(5.00000, 105.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "2.00000+102.00000i", "3.00000+103.00000i", "4.00000+104.00000i", "5.00000+105.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_mvccTable(t *testing.T) {
	Convey("Test_Table_mvccTable:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n = 6;timev = take(2022.01.03T12:00:00+1..n, n);num = take(1023.002+1..n, n);name = take(`a`b`c`d`e`f, n);boolv = take(true false , n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t=mvccTable(timev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);select * from t")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1024.002, 1025.002, 1026.002, 1027.002, 1028.002, 1029.002}
		name := []string{"a", "b", "c", "d", "e", "f"}
		boolv := []bool{true, false, true, false, true, false}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(2.00000, 102.00000)", "(3.00000, 103.00000)", "(4.00000, 104.00000)", "(5.00000, 105.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "2.00000+102.00000i", "3.00000+103.00000i", "4.00000+104.00000i", "5.00000+105.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_cachedTable(t *testing.T) {
	Convey("Test_Table_cachedTable:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("def f1(mutable t){return t};n = 6;timev = take(2022.01.03T12:00:00+1..n, n);num = take(1023.002+1..n, n);name = take(`a`b`c`d`e`f, n);boolv = take(true false , n);uuidv = take(uuid('a268652a-6c8e-5686-5dd9-4ab882ecb969'), n);ipaddrv = take(ipaddr('191.168.13.16'), n);int128v = take(int128('97b48f09119a1d91d44fd12893226af8'), n);pointv = take(point(0.0+1..n,100.0+1..n), n);complexv = take(complex(0.0+1..n,100.0+1..n), n);t=table(timev, num, name, boolv, uuidv, ipaddrv, int128v, pointv, complexv);ct=cachedTable(f1{t}, 2);select * from ct;")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		datetimev := []time.Time{time.Date(2022, 1, 3, 12, 00, 1, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 2, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 3, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 4, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 5, 0, time.UTC), time.Date(2022, 1, 3, 12, 00, 6, 0, time.UTC)}
		num := []float64{1024.002, 1025.002, 1026.002, 1027.002, 1028.002, 1029.002}
		name := []string{"a", "b", "c", "d", "e", "f"}
		boolv := []bool{true, false, true, false, true, false}
		uuidv := string("a268652a-6c8e-5686-5dd9-4ab882ecb969")
		ipaddrv := string("191.168.13.16")
		int128v := string("97b48f09119a1d91d44fd12893226af8")
		pointv := []string{"(1.00000, 101.00000)", "(2.00000, 102.00000)", "(3.00000, 103.00000)", "(4.00000, 104.00000)", "(5.00000, 105.00000)", "(6.00000, 106.00000)"}
		complexv := []string{"1.00000+101.00000i", "2.00000+102.00000i", "3.00000+103.00000i", "4.00000+104.00000i", "5.00000+105.00000i", "6.00000+106.00000i"}
		col := result.Columns()
		So(col, ShouldEqual, 9)
		row := result.Rows()
		So(row, ShouldEqual, 6)
		for i := 0; i < result.Columns(); i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, datetimev[j])
					}
				}
			case 1:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, num[j])
					}
				}
			case 2:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, name[j])
					}
				}
			case 3:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, boolv[j])
					}
				}
			case 4:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, uuidv)
					}
				}
			case 5:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, ipaddrv)
					}
				}
			case 6:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, int128v)
					}
				}
			case 7:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, pointv[j])
					}
				}
			case 8:
				{
					for j := 0; j < result.Rows(); j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, complexv[j])
					}
				}
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_with_bigArray(t *testing.T) {
	Convey("Test_Table_with_bigArray:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n=9000000;id = bigarray(INT,0,n).append!(take(long(1..n),n));name = bigarray(SYMBOL,0,n).append!(take(`A`S`B`C`D, n));table1 = (table(id, name));select * from table1")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		col := result.Columns()
		So(col, ShouldEqual, 2)
		row := result.Rows()
		So(row, ShouldEqual, 9000000)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_DownLoad_with_wide_table(t *testing.T) {
	Convey("Test_Table_with_wide_table:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a = take([(`A)+string(1..10)],10000);a.append!(`A1`a2`a3```````a10);a.append!(``````````);t = table(a);t")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 10)
		col := result.Columns()
		So(col, ShouldEqual, 10002)
		So(db.Close(), ShouldBeNil)
		zx := []string{"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"}
		re := result.GetColumnByIndex(100)
		for j := 0; j < row; j++ {
			re := re.Get(j).Value()
			So(re, ShouldEqual, zx[j])
		}
		zx = []string{"A1", "a2", "a3", "", "", "", "", "", "", "a10"}
		re = result.GetColumnByIndex(10000)
		for j := 0; j < row; j++ {
			re := re.Get(j).Value()
			So(re, ShouldEqual, zx[j])
		}
		zx = []string{"", "", "", "", "", "", "", "", "", ""}
		re = result.GetColumnByIndex(10001)
		for j := 0; j < row; j++ {
			re := re.Get(j).Value()
			So(re, ShouldEqual, zx[j])
		}
	})
}
func Test_Table_DownLoad_with_array_vector(t *testing.T) {
	Convey("Test_Table_with_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("bid = array(DOUBLE[], 0, 20).append!([1.4799 NULL 1.4787, , 1.4791 1.479 1.4784]);t = table( bid as `bid);t")
		So(err, ShouldBeNil)
		result := s.(*model.Table)
		row := result.Rows()
		So(row, ShouldEqual, 3)
		col := result.Columns()
		So(col, ShouldEqual, 1)
		So(db.Close(), ShouldBeNil)
		hasnull := []float64{1.4799, 0, 1.4787}
		notnull := []float64{1.4791, 1.479, 1.4784}
		for i := 0; i < col; i++ {
			re := result.GetColumnByIndex(i)
			switch i {
			case 0:
				{
					for j := 0; j < row; j++ {
						if j == 1 {
							So(re.IsNull(j), ShouldBeTrue)
						} else {
							re := re.Get(j).Value()
							So(re, ShouldEqual, hasnull[j])
						}
					}
				}
			case 1:
				{
					for j := 0; j < row; j++ {
						So(re.IsNull(j), ShouldBeTrue)
					}
				}
			case 2:
				{
					for j := 0; j < row; j++ {
						re := re.Get(j).Value()
						So(re, ShouldEqual, notnull[j])
					}
				}
			}
		}
	})
}
func Test_Table_UpLoad_DataType_string(t *testing.T) {
	Convey("Test_Table_upload_with_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_string:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtString, []string{"col1", "col2", "col3"})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []string{"col1", "col2", "col3"}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(IN-MEMORY TABLE)")
			So(res.GetDataType(), ShouldEqual, model.DtVoid)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_int(t *testing.T) {
	Convey("Test_Table_upload_with_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_int:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{1024, model.NullInt, 369})
			So(err, ShouldBeNil)
			col1, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{147, 258, 369})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col", "ss"}, []*model.Vector{model.NewVector(col), model.NewVector(col1)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []int32{1024, math.MinInt32, 369}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
			So(ty.String(), ShouldEqual, "string(IN-MEMORY TABLE)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_char(t *testing.T) {
	Convey("Test_Table_upload_with_char:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_char:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtChar, []byte{127, 2, 13})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []byte{127, 2, 13}
			for j := 0; j < 3; j++ {
				re := re.Get(j).Value()
				So(re, ShouldEqual, zx[j])
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_short(t *testing.T) {
	Convey("Test_Table_upload_with_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_short:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtShort, []int16{127, -12552, 1024})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []int16{127, -12552, 1024}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_long(t *testing.T) {
	Convey("Test_Table_upload_with_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_long:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtLong, []int64{1048576, -1024, 13169})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []int64{1048576, -1024, 13169}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_float(t *testing.T) {
	Convey("Test_Table_upload_with_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_float:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtFloat, []float32{1048576.02, -1024.365, 13169.14196})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []float32{1048576.02, -1024.365, 13169.14196}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_double(t *testing.T) {
	Convey("Test_Table_upload_with_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_double:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{1048576.02011, -1024.365, 13169.14196})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []float64{1048576.02011, -1024.365, 13169.14196}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_bool(t *testing.T) {
	Convey("Test_Table_upload_with_bool:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_bool:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtBool, []byte{1, 0, 1})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []bool{true, false, true}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_date(t *testing.T) {
	Convey("Test_Table_upload_with_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_date:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_month(t *testing.T) {
	Convey("Test_Table_upload_with_month:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_month:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_time(t *testing.T) {
	Convey("Test_Table_upload_with_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_time:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_minute(t *testing.T) {
	Convey("Test_Table_upload_with_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_minute:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_second(t *testing.T) {
	Convey("Test_Table_upload_with_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_second:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_datetime(t *testing.T) {
	Convey("Test_Table_upload_with_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_datetime:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_timestamp(t *testing.T) {
	Convey("Test_Table_upload_with_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_timestamp:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_nanotime(t *testing.T) {
	Convey("Test_Table_upload_with_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_nanotime:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_nanotimestamp(t *testing.T) {
	Convey("Test_Table_upload_with_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_nanotimestamp:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_datehour(t *testing.T) {
	Convey("Test_Table_upload_with_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_datehour:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_blob(t *testing.T) {
	Convey("Test_Table_upload_with_blob:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_blob:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtBlob, [][]byte{{6}, {12}, {56}, {128}})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := [][]byte{{6}, {12}, {56}, {128}}
			for j := 0; j < 3; j++ {
				re := re.Get(j).Value()
				So(re, ShouldResemble, zx[j])
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_uuid(t *testing.T) {
	Convey("Test_Table_upload_with_uuid:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_uuid:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_ipaddr(t *testing.T) {
	Convey("Test_Table_upload_with_ipaddr:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_ipaddr:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtIP, []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_int128(t *testing.T) {
	Convey("Test_Table_upload_with_int128:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_int128:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_point(t *testing.T) {
	Convey("Test_Table_upload_with_point:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_point:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtPoint, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"col"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []string{"(1.00000, 1.00000)", "(-1.00000, -1024.50000)", "(1001022.40000, -30028.75000)"}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_complex(t *testing.T) {
	Convey("Test_Table_upload_with_complex:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_complex:", func() {
			col, err := model.NewDataTypeListWithRaw(model.DtComplex, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"complex"}, []*model.Vector{model.NewVector(col)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table).GetColumnByIndex(0)
			zx := []string{"1.00000+1.00000i", "-1.00000+-1024.50000i", "1001022.40000+-30028.75000i"}
			var k int
			for i := 0; i < int(re.RowCount); i++ {
				if re.Get(i).Value() == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, re.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_DataType_has_all_type_part1(t *testing.T) {
	Convey("Test_Table_upload_with_all_datatype:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_Table_with_all_datatype:", func() {
			complexv, err := model.NewDataTypeListWithRaw(model.DtComplex, [][2]float64{{1, 1}, model.NullComplex, {1001022.4, -30028.75}})
			So(err, ShouldBeNil)
			stringv, err := model.NewDataTypeListWithRaw(model.DtString, []string{"col1", "", "col3"})
			So(err, ShouldBeNil)
			intv, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{1024, model.NullInt, 369})
			So(err, ShouldBeNil)
			charv, err := model.NewDataTypeListWithRaw(model.DtChar, []byte{127, model.NullChar, 13})
			So(err, ShouldBeNil)
			shortv, err := model.NewDataTypeListWithRaw(model.DtShort, []int16{127, model.NullShort, 1024})
			So(err, ShouldBeNil)
			longv, err := model.NewDataTypeListWithRaw(model.DtLong, []int64{1048576, model.NullLong, 13169})
			So(err, ShouldBeNil)
			floatv, err := model.NewDataTypeListWithRaw(model.DtFloat, []float32{1048576.02, model.NullFloat, 13169.14196})
			So(err, ShouldBeNil)
			doublev, err := model.NewDataTypeListWithRaw(model.DtDouble, []float64{1048576.02011, model.NullDouble, 13169.14196})
			So(err, ShouldBeNil)
			boolv, err := model.NewDataTypeListWithRaw(model.DtBool, []byte{1, model.NullBool, 1})
			So(err, ShouldBeNil)
			pointv, err := model.NewDataTypeListWithRaw(model.DtPoint, [][2]float64{{1, 1}, model.NullPoint, {1001022.4, -30028.75}})
			So(err, ShouldBeNil)
			symbolv, err := model.NewDataTypeListWithRaw(model.DtSymbol, []string{"*", "", "87"})
			So(err, ShouldBeNil)
			datev, err := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			monthv, err := model.NewDataTypeListWithRaw(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			timev, err := model.NewDataTypeListWithRaw(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			minutev, err := model.NewDataTypeListWithRaw(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			secondv, err := model.NewDataTypeListWithRaw(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			datetimev, err := model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			timestampv, err := model.NewDataTypeListWithRaw(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			nanotimev, err := model.NewDataTypeListWithRaw(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			nanotimestampv, err := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			datehourv, err := model.NewDataTypeListWithRaw(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), model.NullTime, time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			uuidv, err := model.NewDataTypeListWithRaw(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "", "5d212a78-cc48-e3b1-4235-b4d91473ee89"})
			So(err, ShouldBeNil)
			ipaddrv, err := model.NewDataTypeListWithRaw(model.DtIP, []string{"192.163.1.12", "", "127.0.0.1"})
			So(err, ShouldBeNil)
			int128v, err := model.NewDataTypeListWithRaw(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "", "e1671797c52e15f763380b45e841ec34"})
			So(err, ShouldBeNil)
			tb := model.NewTable([]string{"complex", "string", "int", "char", "short", "long", "float", "double", "point", "bool", "date", "month", "time", "minute", "second", "datetime", "timestamp", "nanotime", "nanotimestamp", "datehour", "uuid", "ipaddr", "int128", "symbol"}, []*model.Vector{model.NewVector(complexv), model.NewVector(stringv), model.NewVector(intv), model.NewVector(charv), model.NewVector(shortv), model.NewVector(longv), model.NewVector(floatv), model.NewVector(doublev), model.NewVector(pointv), model.NewVector(boolv), model.NewVector(datev), model.NewVector(monthv), model.NewVector(timev), model.NewVector(minutev), model.NewVector(secondv), model.NewVector(datetimev), model.NewVector(timestampv), model.NewVector(nanotimev), model.NewVector(nanotimestampv), model.NewVector(datehourv), model.NewVector(uuidv), model.NewVector(ipaddrv), model.NewVector(int128v), model.NewVector(symbolv)})
			_, err = db.Upload(map[string]model.DataForm{"s": tb})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			re := res.(*model.Table)
			complexvs := []string{"1.00000+1.00000i", "", "1001022.40000+-30028.75000i"}
			intvs := []int32{1024, model.NullInt, 369}
			stringvs := []string{"col1", "", "col3"}
			charvs := []byte{127, model.NullChar, 13}
			shortvs := []int16{127, model.NullShort, 1024}
			longvs := []int64{1048576, model.NullLong, 13169}
			floatvs := []float32{1048576.02, model.NullFloat, 13169.14196}
			doublevs := []float64{1048576.02011, model.NullDouble, 13169.14196}
			boolvs := []bool{true, false, true}
			pointvs := []string{"(1.00000, 1.00000)", "(,)", "(1001022.40000, -30028.75000)"}
			datevs := []time.Time{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			monthvs := []time.Time{time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			timevs := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			minutevs := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}
			secondvs := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			datetimevs := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			symbolvs := []string{"*", "", "87"}
			timestampvs := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			nanotimevs := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			nanotimestampvs := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			datehourvs := []time.Time{time.Date(2022, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			uuidvs := []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			ipaddrvs := []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"}
			int128vs := []string{"e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000", "e1671797c52e15f763380b45e841ec34"}
			re2 := re.GetColumnByIndex(3)
			for j := 0; j < 3; j++ {
				if j == 1 {
					So(re2.Get(j).IsNull(), ShouldBeTrue)
				} else {
					So(re2.Get(j).Value(), ShouldEqual, charvs[j])
				}
			}
			re1 := res.(*model.Table).GetColumnByIndex(0)
			var k int
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == complexvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(1)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == stringvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(2)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == intvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(4)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == shortvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(5)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == longvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(6)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == floatvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(7)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == doublevs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			boolV := re.GetColumnByIndex(9)
			for j := 0; j < 3; j++ {
				if j == 1 {
					So(boolV.IsNull(j), ShouldBeTrue)
				} else {
					re := boolV.Get(j).Value()
					So(re, ShouldEqual, boolvs[j])
				}
			}
			re1 = res.(*model.Table).GetColumnByIndex(8)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == pointvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(10)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == datevs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(11)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == monthvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(12)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == timevs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(13)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == minutevs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(14)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == secondvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(15)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == datetimevs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(16)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == timestampvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(17)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == nanotimevs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(18)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == nanotimestampvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(19)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == datehourvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(20)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == uuidvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(21)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == ipaddrvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(22)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == int128vs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
			re1 = res.(*model.Table).GetColumnByIndex(23)
			k = 0
			for i := 0; i < int(re1.RowCount); i++ {
				if re1.Get(i).Value() == symbolvs[i] {
					k++
				}
			}
			So(k, ShouldEqual, re1.RowCount)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Table_UpLoad_big_array(t *testing.T) {
	Convey("Test_Table_upload_with_big_array:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		var i int32
		intv := []int32{}
		for i = 0; i < 3000000*12; i += 12 {
			intv = append(intv, i)
		}
		intv = append(intv, model.NullInt)
		col, err := model.NewDataTypeListWithRaw(model.DtInt, intv)
		So(err, ShouldBeNil)
		stringv := []string{}
		for i = 0; i < 3000000*12; i += 12 {
			stringv = append(stringv, string("hello"))
		}
		stringv = append(stringv, model.NullString)
		col1, err := model.NewDataTypeListWithRaw(model.DtString, stringv)
		So(err, ShouldBeNil)
		allnull := []string{}
		for i = 0; i < 3000001*12; i += 12 {
			allnull = append(allnull, model.NullString)
		}
		allnullv, err := model.NewDataTypeListWithRaw(model.DtString, allnull)
		So(err, ShouldBeNil)
		tb := model.NewTable([]string{"int_v", "str_v", "all_null"}, []*model.Vector{model.NewVector(col), model.NewVector(col1), model.NewVector(allnullv)})
		_, err = db.Upload(map[string]model.DataForm{"s": tb})
		So(err, ShouldBeNil)
		res, _ := db.RunScript("s")
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Table).GetColumnByIndex(0)
		var k int
		for i := 0; i < int(re.RowCount); i++ {
			if re.Get(i).Value() == intv[i] {
				k++
			}
		}
		So(k, ShouldEqual, re.RowCount)
		re = res.(*model.Table).GetColumnByIndex(1)
		k = 0
		for i := 0; i < int(re.RowCount); i++ {
			if re.Get(i).Value() == stringv[i] {
				k++
			}
		}
		So(k, ShouldEqual, re.RowCount)
		re = res.(*model.Table).GetColumnByIndex(2)
		k = 0
		for i := 0; i < int(re.RowCount); i++ {
			if re.Get(i).Value() == allnull[i] {
				k++
			}
		}
		So(k, ShouldEqual, re.RowCount)
		So(ty.String(), ShouldEqual, "string(IN-MEMORY TABLE)")
		So(db.Close(), ShouldBeNil)
	})
}
