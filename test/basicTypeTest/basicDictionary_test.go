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

func Test_Dictionary_DownLoad_int(t *testing.T) {
	Convey("Test_dictionary_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_int_not_null:", func() {
			s, err := db.RunScript("x=2 -6 1024 1048576 -2019;y=4875 -23 1048576 666 -2205;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"2", "-6", "1024", "1048576", "-2019"}
			val := [5]int32{4875, -23, 1048576, 666, -2205}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 5)
		})
		Convey("Test_dictionary_int_null_values:", func() {
			s, err := db.RunScript("x=2 6 1 5 9;y=take(00i,5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"2", "6", "1", "5", "9"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		Convey("Test_dictionary_int_all_null:", func() {
			s, err := db.RunScript("x = take(00i,6);y= take(00i,6);z = dict(x,y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_short(t *testing.T) {
	Convey("Test_dictionary_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_short_not_null:", func() {
			s, err := db.RunScript("x=2h -6h 1024h 4875h -2019h;y=4h 3333h 6666h 8888h -5h;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"2", "-6", "1024", "4875", "-2019"}
			val := [5]int16{4, 3333, 6666, 8888, -5}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		Convey("Test_dictionary_short_null_values:", func() {
			s, err := db.RunScript("x=2h 6h 1h 5h 9h;y=take(00h,5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"2", "6", "1", "5", "9"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_char(t *testing.T) {
	Convey("Test_dictionary_char:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_char_not_null:", func() {
			s, err := db.RunScript("x=97c 98c 99c 100c 101c ;y=102c 103c 104c 105c 106c ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			re := result.KeyStrings()
			key := [5]string{"97", "98", "99", "100", "101"}
			val := [5]uint{102, 103, 104, 105, 106}
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				So(zx, ShouldEqual, val[i])
				So(re[i], ShouldBeIn, key)
				get1, _ := result.Get(re[i])
				zx1 := get1.Value()
				if v, ok := zx1.(int); ok {
					So(val, ShouldContain, v)
				}
				reType := result.GetDataType()
				So(reType, ShouldEqual, 2)
				reTypeString := result.GetDataTypeString()
				So(reTypeString, ShouldEqual, "char")
			}
		})
		Convey("Test_dictionary_char_null_values:", func() {
			s, err := db.RunScript("x=97c 98c 99c 100c 101c;y=take(char(['','','','','']),5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			re := result.KeyStrings()
			key := [5]string{"97", "98", "99", "100", "101"}
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				So(get.IsNull(), ShouldEqual, true)
				So(re[i], ShouldBeIn, key)
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_symbol(t *testing.T) {
	Convey("Test_dictionary_symbol:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_symbol_not_null:", func() {
			s, err := db.RunScript("x=symbol(`A`B`C`D`E) ;y=symbol(`Z`X`C`V`B) ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"A", "B", "C", "D", "E"}
			val := [5]string{"Z", "X", "C", "V", "B"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 17)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbol")
		})
		Convey("Test_dictionary_symbol_null_values:", func() {
			s, err := db.RunScript("x=symbol(`A`B`C`D`E) ;y=take(symbol(['','','','','']),5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			re := result.KeyStrings()
			key := [5]string{"A", "B", "C", "D", "E"}
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				So(get.IsNull(), ShouldEqual, false)
				So(re[i], ShouldBeIn, key)
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 145)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbolExtend")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_string(t *testing.T) {
	Convey("Test_dictionary_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_string_not_null:", func() {
			s, err := db.RunScript("x= `IBM`C`MS`MSFT`JPM;y=`C`MS`MSFT`JPM`ORCL ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)

			key := [5]string{"IBM", "C", "MS", "MSFT", "JPM"}
			val := [5]string{"C", "MS", "MSFT", "JPM", "ORCL"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
		})
		Convey("Test_dictionary_string_null_values:", func() {
			s, err := db.RunScript("x= `IBM`C`MS`MSFT`JPM ;y=take((['','','','','']),5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"IBM", "C", "MS", "MSFT", "JPM"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_long(t *testing.T) {
	Convey("Test_dictionary_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_long:", func() {
			s, err := db.RunScript("x=97l -98l 1024l 1048576l -101110l ;y=-1102l 110103l 1024l -112105l 1048576l ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"97", "-98", "1024", "1048576", "-101110"}
			val := [5]int64{-1102, 110103, 1024, -112105, 1048576}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		Convey("Test_dictionary_long_null_values:", func() {
			s, err := db.RunScript("x=97l 98l 99l 100l 101l;y=take(00l,5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"97", "98", "99", "100", "101"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_double(t *testing.T) {
	Convey("Test_dictionary_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_double_not_null:", func() {
			s, err := db.RunScript("x=97.5 -98.5 1099.5 148576.5 101111.5 ;y=102.5 103.5 104.5 105.5 106.5 ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"97.5", "-98.5", "1099.5", "148576.5", "101111.5"}
			val := [5]float64{102.5, 103.5, 104.5, 105.5, 106.5}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		Convey("Test_dictionary_double_null_values:", func() {
			s, err := db.RunScript("x=97.5 98.5 99.5 100.5 101.5 ;y=take(double(['','','','','']),5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"97.5", "98.5", "99.5", "100.5", "101.5"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_float(t *testing.T) {
	Convey("Test_dictionary_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_float_not_null:", func() {
			s, err := db.RunScript("x=97f -98f 99f 1024f 104857f ;y=102f 103f 104f 105f 106f ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"97", "-98", "99", "1024", "104857"}
			val := [5]float32{102, 103, 104, 105, 106}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		Convey("Test_dictionary_float_null_values:", func() {
			s, err := db.RunScript("x=97f 98f 99f 100f 101f;y=take(00f,5);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [5]string{"97", "98", "99", "100", "101"}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_date(t *testing.T) {
	Convey("Test_dictionary_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_date_not_null:", func() {
			s, err := db.RunScript("x=1969.12.31 1970.01.01 1970.01.02 2006.01.02 2006.01.03 2022.08.03 ;y=1969.12.31 1970.01.01 1970.01.02 2006.01.02 2006.01.03 2022.08.03;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"1969.12.31", "1970.01.01", "1970.01.02", "2006.01.02", "2006.01.03", "2022.08.03"}
			val := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		Convey("Test_dictionary_date_null_values:", func() {
			s, err := db.RunScript("x=2022.08.03+take(100..105,6);y=take(00d,6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"2022.11.11", "2022.11.12", "2022.11.13", "2022.11.14", "2022.11.15", "2022.11.16"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_month(t *testing.T) {
	Convey("Test_dictionary_month:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_month_not_null:", func() {
			s, err := db.RunScript("x=1969.12M 1970.01M 1970.02M 2006.01M 2006.02M 2022.08M ;y=1969.12M 1970.01M 1970.02M 2006.01M 2006.02M 2022.08M;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"1969.12M", "1970.01M", "1970.02M", "2006.01M", "2006.02M", "2022.08M"}
			val := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 1, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_dictionary_month_null_values:", func() {
			s, err := db.RunScript("x=2012.08M+take(100..105,6);y=take(10+month(['','','','','','']),6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"2020.12M", "2021.01M", "2021.02M", "2021.03M", "2021.04M", "2021.05M"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_date_time(t *testing.T) {
	Convey("Test_dictionary_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_time_not_null:", func() {
			s, err := db.RunScript("x=23:59:59.999 00:00:00.000 00:00:01.999 15:04:04.999 15:04:05.000 15:00:15.000 ;y=23:59:59.999 00:00:00.000 00:00:01.999 15:04:04.999 15:04:05.000 15:00:15.000;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"23:59:59.999", "00:00:00.000", "00:00:01.999", "15:04:04.999", "15:04:05.000", "15:00:15.000"}
			val := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		Convey("Test_dictionary_time_null_values:", func() {
			s, err := db.RunScript("x=09:30:00.000+take(100..105,6);y=take(10+time(['','','','','','']),6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"09:30:00.100", "09:30:00.101", "09:30:00.102", "09:30:00.103", "09:30:00.104", "09:30:00.105"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_minute(t *testing.T) {
	Convey("Test_dictionary_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_minute_not_null:", func() {
			s, err := db.RunScript("x=23:59m 00:00m 00:01m 15:04m 15:05m 15:15m ;y=23:59m 00:00m 00:01m 15:04m 15:05m 15:15m;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"23:59m", "00:00m", "00:01m", "15:04m", "15:05m", "15:15m"}
			val := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 1, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 5, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 15, 0, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		Convey("Test_dictionary_minute_null_values:", func() {
			s, err := db.RunScript("x=09:30m+take(100..105,6);y=take(00m,6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"11:10m", "11:11m", "11:12m", "11:13m", "11:14m", "11:15m"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_second(t *testing.T) {
	Convey("Test_dictionary_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_second_not_null:", func() {
			s, err := db.RunScript("x=23:59:59 00:00:00 00:00:01 15:04:04 15:04:05 15:00:15 ;y=23:59:59 00:00:00 00:00:01 15:04:04 15:04:05 15:00:15;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"23:59:59", "00:00:00", "00:00:01", "15:04:04", "15:04:05", "15:00:15"}
			val := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		Convey("Test_dictionary_second_null_values:", func() {
			s, err := db.RunScript("x=09:30:00+take(100..105,6);y=take(00s,6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"09:31:40", "09:31:41", "09:31:42", "09:31:43", "09:31:44", "09:31:45"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_datetime(t *testing.T) {
	Convey("Test_dictionary_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_datetime_not_null:", func() {
			s, err := db.RunScript("x = 1969.12.31T23:59:59 1970.01.01T00:00:00 1970.01.01T00:00:01 2006.01.02T15:04:04 2006.01.02T15:04:05 2022.08.03T15:00:15 ;y=1969.12.31T23:59:59 1970.01.01T00:00:00 1970.01.01T00:00:01 2006.01.02T15:04:04 2006.01.02T15:04:05 2022.08.03T15:00:15;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"1969.12.31T23:59:59", "1970.01.01T00:00:00", "1970.01.01T00:00:01", "2006.01.02T15:04:04", "2006.01.02T15:04:05", "2022.08.03T15:00:15"}
			val := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		Convey("Test_dictionary_datetime_null_values:", func() {
			s, err := db.RunScript("x=2022.08.03 11:00:00+take(10..15,6);y=take(datetime(['','','','','','']),6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"2022.08.03T11:00:10", "2022.08.03T11:00:11", "2022.08.03T11:00:12", "2022.08.03T11:00:13", "2022.08.03T11:00:14", "2022.08.03T11:00:15"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_timestamp(t *testing.T) {
	Convey("Test_dictionary_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_timestamp_not_null:", func() {
			s, err := db.RunScript("x=1969.12.31T23:59:59.999 1970.01.01T00:00:00.000 1970.01.01T00:00:01.999 2006.01.02T15:04:04.999 2006.01.02T15:04:05.000 2022.08.03T15:00:15.000;y=1969.12.31T23:59:59.999 1970.01.01T00:00:00.000 1970.01.01T00:00:01.999 2006.01.02T15:04:04.999 2006.01.02T15:04:05.000 2022.08.03T15:00:15.000;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"1969.12.31T23:59:59.999", "1970.01.01T00:00:00.000", "1970.01.01T00:00:01.999", "2006.01.02T15:04:04.999", "2006.01.02T15:04:05.000", "2022.08.03T15:00:15.000"}
			val := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		Convey("Test_dictionary_timestamp_null_values:", func() {
			s, err := db.RunScript("x=2012.08.03 11:00:00.000+take(100..105,6);y=take(timestamp(['','','','','','']),6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"2012.08.03T11:00:00.100", "2012.08.03T11:00:00.101", "2012.08.03T11:00:00.102", "2012.08.03T11:00:00.103", "2012.08.03T11:00:00.104", "2012.08.03T11:00:00.105"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_nanotime(t *testing.T) {
	Convey("Test_dictionary_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_nanotime_not_time:", func() {
			s, err := db.RunScript("x=23:59:59.999999999 00:00:00.000000000 00:00:01.999999999 15:04:04.999999999 15:04:05.000000000 15:00:15.000000000 ;y=23:59:59.999999999 00:00:00.000000000 00:00:01.999999999 15:04:04.999999999 15:04:05.000000000 15:00:15.000000000;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"23:59:59.999999999", "00:00:00.000000000", "00:00:01.999999999", "15:04:04.999999999", "15:04:05.000000000", "15:00:15.000000000"}
			val := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		Convey("Test_dictionary_nanotime_null_values:", func() {
			s, err := db.RunScript("x=11:00:00.000000000+take(100..105,6);y=take(nanotime(['','','','','','']),6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"11:00:00.000000100", "11:00:00.000000101", "11:00:00.000000102", "11:00:00.000000103", "11:00:00.000000104", "11:00:00.000000105"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_nanotimestamp(t *testing.T) {
	Convey("Test_dictionary_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_nanotimestamp_not_time:", func() {
			s, err := db.RunScript("x=1969.12.31T23:59:59.999999999 1970.01.01T00:00:00.000000000 1970.01.01T00:00:01.999999999 2006.01.02T15:04:04.999999999 2006.01.02T15:04:05.000000000 2022.08.03T15:00:15.000000000 ;y=1969.12.31T23:59:59.999999999 1970.01.01T00:00:00.000000000 1970.01.01T00:00:01.999999999 2006.01.02T15:04:04.999999999 2006.01.02T15:04:05.000000000 2022.08.03T15:00:15.000000000;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"1969.12.31T23:59:59.999999999", "1970.01.01T00:00:00.000000000", "1970.01.01T00:00:01.999999999", "2006.01.02T15:04:04.999999999", "2006.01.02T15:04:05.000000000", "2022.08.03T15:00:15.000000000"}
			val := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		Convey("Test_dictionary_nanotimestamp_null_values:", func() {
			s, err := db.RunScript("x=2022.08.03 11:00:00.000000000+take(100..105,6);y=take(nanotimestamp(['','','','','','']),6);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [6]string{"2022.08.03T11:00:00.000000100", "2022.08.03T11:00:00.000000101", "2022.08.03T11:00:00.000000102", "2022.08.03T11:00:00.000000103", "2022.08.03T11:00:00.000000104", "2022.08.03T11:00:00.000000105"}
			var k int
			for i := 0; i < 6; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_DownLoad_datehour(t *testing.T) {
	Convey("Test_dictionary_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_datehour_not_null:", func() {
			s, err := db.RunScript("x = datehour[1969.12.31T23:59:59.999, 1970.01.01T00:00:00.000, 2006.01.02T15:04:04.999] ;y=datehour[1969.12.31T23:59:59.999, 1970.01.01T00:00:00.000, 2006.01.02T15:04:04.999] ;z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [3]string{"1969.12.31T23", "1970.01.01T00", "2006.01.02T15"}
			val := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datehour")
		})
		Convey("Test_dictionary_datehour_null_values:", func() {
			s, err := db.RunScript("x=datehour([2022.07.29 15:00:00.000, 2022.07.29 16:00:00.000, 2022.07.29 17:00:00.000]);y=take(datehour(['','','']),3);z=dict(x, y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := [3]string{"2022.07.29T15", "2022.07.29T16", "2022.07.29T17"}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				if get.IsNull() == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datehour")
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Dictionary_DownLoad_decimal32(t *testing.T) {
	Convey("Test_dictionary_decimal32:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_decimal32_not_null:", func() {
			s, err := db.RunScript("x=`a`b`c`d`e;y=take(0.99999,5)$DECIMAL32(2);z=dict(x,y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := []string{"a", "b", "c", "d"}
			val := &model.Decimal32s{2, []float64{0.99, 0.99, 0.99}}
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value().(*model.Scalar).Value().(*model.Decimal32)
				// fmt.Println(zx.Scale, zx.Value, val.Value[i])
				So(zx.Scale, ShouldEqual, val.Scale)
				So(zx.Value, ShouldEqual, val.Value[i])
			}
		})
		Convey("Test_dictionary_decimal32_null_values:", func() {
			s, err := db.RunScript("x=`a`b`c`d`e;y=take(decimal32(NULL,5),5);z=dict(x,y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := []string{"a", "b", "c", "d"}
			val := &model.Decimal32s{5, []float64{model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value}}
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				So(get.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
				zx := get.Value().(*model.Scalar).Value().(*model.Decimal32)
				// fmt.Println(zx.Scale, zx.Value, val.Value)
				So(zx.Scale, ShouldEqual, val.Scale)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Dictionary_DownLoad_decimal64(t *testing.T) {
	Convey("Test_dictionary_decimal64:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_decimal64_not_null:", func() {
			s, err := db.RunScript("x=`a`b`c`d`e;y=take(0.999994564855,5)$DECIMAL64(11);z=dict(x,y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := []string{"a", "b", "c", "d"}
			val := &model.Decimal64s{11, []float64{0.99999456485, 0.99999456485, 0.99999456485}}
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value().(*model.Scalar).Value().(*model.Decimal64)
				// fmt.Println(zx.Scale, zx.Value, val.Value[i])
				So(zx.Scale, ShouldEqual, val.Scale)
				So(zx.Value, ShouldEqual, val.Value[i])
			}
		})
		Convey("Test_dictionary_decimal64_null_values:", func() {
			s, err := db.RunScript("x=`a`b`c`d`e;y=take(decimal64(NULL,5),5);z=dict(x,y);z")
			So(err, ShouldBeNil)
			result := s.(*model.Dictionary)
			key := []string{"a", "b", "c", "d"}
			val := &model.Decimal64s{5, []float64{model.NullDecimal64Value, model.NullDecimal64Value, model.NullDecimal64Value}}
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				So(get.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
				zx := get.Value().(*model.Scalar).Value().(*model.Decimal64)
				// fmt.Println(zx.Scale, zx.Value, val.Value[i])
				So(zx.Scale, ShouldEqual, val.Scale)
			}
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Dictionary_UpLoad_int_and_long(t *testing.T) {
	Convey("Test_dictionary_int->long_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_int->long:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, -6, 1024, 1048576, -2019})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{4875, -23, 1048576, 666, -2205})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := [5]string{"2", "-6", "1024", "1048576", "-2019"}
			val := [5]int64{4875, -23, 1048576, 666, -2205}
			var k int
			for i := 0; i < 5; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			So(ty.String(), ShouldEqual, "string(INT->LONG DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_short_and_char(t *testing.T) {
	Convey("Test_dictionary_short->char_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_short->char:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{2, -6, 102})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{48, 23, 10})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			re := result.KeyStrings()
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"2", "-6", "102"}
			val := []byte{48, 23, 10}
			for i := 0; i < len(re); i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				So(zx, ShouldEqual, val[i])
				So(re[i], ShouldBeIn, key)
				get1, _ := result.Get(re[i])
				zx1 := get1.Value()
				if v, ok := zx1.(int); ok {
					So(val, ShouldContain, v)
				}
			}
			So(ty.String(), ShouldEqual, "string(SHORT->CHAR DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_long_and_float(t *testing.T) {
	Convey("Test_dictionary_long->float_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_long->float:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{1522542, -1768546, 2022102})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{48.10485, 278953.6, 5454.1515})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"1522542", "-1768546", "2022102"}
			val := []float32{48.10485, 278953.6, 5454.1515}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			So(ty.String(), ShouldEqual, "string(LONG->FLOAT DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_double_and_date(t *testing.T) {
	Convey("Test_dictionary_double->date_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_double->date:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{1522.12, -1766.321, 2102.5454})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"1522.12", "-1766.321", "2102.5454"}
			val := []time.Time{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			So(ty.String(), ShouldEqual, "string(DOUBLE->DATE DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_month_and_time(t *testing.T) {
	Convey("Test_dictionary_month->time_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_double->date:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"2022.12M", "1969.12M", "2006.01M"}
			val := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			So(ty.String(), ShouldEqual, "string(MONTH->TIME DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_minute_and_second(t *testing.T) {
	Convey("Test_dictionary_minute->second_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_minute->second:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"23:59m", "23:59m", "15:04m"}
			val := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, 3)
			So(ty.String(), ShouldEqual, "string(MINUTE->SECOND DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_datetime_and_timestamp(t *testing.T) {
	Convey("Test_dictionary_datetime->timestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_datetime->timestamp:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"2022.12.31T23:59:59", "1969.12.31T23:59:59", "2006.01.02T15:04:04"}
			val := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			So(ty.String(), ShouldEqual, "string(DATETIME->TIMESTAMP DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_nanotime_and_nanotimestamp(t *testing.T) {
	Convey("Test_dictionary_nanotime->nanotimestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_nanotime->nanotimestamp:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"23:59:59.999999999", "23:59:59.999999999", "15:04:04.999999999"}
			val := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, 3)
			So(ty.String(), ShouldEqual, "string(NANOTIME->NANOTIMESTAMP DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_string_and_datehour(t *testing.T) {
	Convey("Test_dictionary_string->datehour_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_string->datehour:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtString, []string{"hello", "%^*", "数据类型"})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"hello", "%^*", "数据类型"}
			val := []time.Time{time.Date(2022, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value()
				if zx == val[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Values.RowCount)
			So(ty.String(), ShouldEqual, "string(STRING->DATEHOUR DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Dictionary_UpLoad_string_and_decimal32(t *testing.T) {
	Convey("Test_dictionary_string->decimal32_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_string->decimal32:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtString, []string{"v1", "v2", "v3"})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{2, []float64{1.33545, -2.3, model.NullDecimal32Value}})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"v1", "v2", "v3"}
			val := &model.Decimal32s{2, []float64{1.33, -2.30, model.NullDecimal32Value}}
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value().(*model.Scalar).Value().(*model.Decimal32)
				if i < 2 {
					So(zx.Scale, ShouldEqual, val.Scale)
					So(zx.Value, ShouldEqual, val.Value[i])
				} else {
					So(get.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
				}
				switch {
				case i == 0:
					So(get.String(), ShouldEqual, "decimal32(1.33)")
				case i == 1:
					So(get.String(), ShouldEqual, "decimal32(-2.30)")
				case i == 2:
					So(get.String(), ShouldEqual, "decimal32()")
				}

			}
			So(ty.String(), ShouldEqual, "string(STRING->ANY DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Dictionary_UpLoad_string_and_decimal64(t *testing.T) {
	Convey("Test_dictionary_string->decimal64_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_string->decimal64:", func() {
			keys, err := model.NewDataTypeListFromRawData(model.DtString, []string{"v1", "v2", "v3"})
			So(err, ShouldBeNil)
			values, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{11, []float64{1.33545, -2.354212356171, model.NullDecimal64Value}})
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			ty, _ := db.RunScript("typestr(s)")
			key := []string{"v1", "v2", "v3"}
			val := &model.Decimal64s{11, []float64{1.33545, -2.35421235617, model.NullDecimal64Value}}
			for i := 0; i < 3; i++ {
				get, _ := result.Get(key[i])
				zx := get.Value().(*model.Scalar).Value().(*model.Decimal64)
				if i < 2 {
					So(zx.Scale, ShouldEqual, val.Scale)
					So(zx.Value, ShouldEqual, val.Value[i])
				} else {
					So(get.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
				}
				switch {
				case i == 0:
					So(get.String(), ShouldEqual, "decimal64(1.33545000000)")
				case i == 1:
					So(get.String(), ShouldEqual, "decimal64(-2.35421235617)")
				case i == 2:
					So(get.String(), ShouldEqual, "decimal64()")
				}
			}
			So(ty.String(), ShouldEqual, "string(STRING->ANY DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Dictionary_UpLoad_big_array_int_and_string(t *testing.T) {
	Convey("Test_dictionary_big_array_int_and_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_dictionary_int->string:", func() {
			var i int32
			intv := []int32{}
			for i = 0; i < 3000000*12; i += 12 {
				intv = append(intv, i)
			}
			intv = append(intv, model.NullInt)
			keys, err := model.NewDataTypeListFromRawData(model.DtInt, intv)
			So(err, ShouldBeNil)
			stringv := []string{}
			for i = 0; i < 3000000*12; i += 12 {
				stringv = append(stringv, string("hello"))
			}
			stringv = append(stringv, model.NullString)
			values, err := model.NewDataTypeListFromRawData(model.DtString, stringv)
			So(err, ShouldBeNil)
			dict := model.NewDictionary(model.NewVector(keys), model.NewVector(values))
			_, err = db.Upload(map[string]model.DataForm{"s": dict})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("s")
			result := res.(*model.Dictionary)
			So(result.Keys.RowCount, ShouldEqual, 3000001)
			So(result.Values.RowCount, ShouldEqual, 3000001)
			ty, _ := db.RunScript("typestr(s)")
			So(ty.String(), ShouldEqual, "string(INT->STRING DICTIONARY)")
		})
		So(db.Close(), ShouldBeNil)
	})
}
