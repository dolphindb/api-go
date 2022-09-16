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

func Test_Vector_Download_Datatype_string(t *testing.T) {
	Convey("Test_vector_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_string_not_null:", func() {
			s, err := db.RunScript("string(`ibm `你好 `yhoo)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
			zx := [3]string{"ibm", "你好", "yhoo"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_vector_string_has_null:", func() {
			s, err := db.RunScript("string(`ibm ` `yhoo)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
			zx := [3]string{"ibm", "", "yhoo"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			result1 := s.(*model.Vector).SetNull
			So(result1, ShouldNotBeNil)
			So(result.Get(1).IsNull(), ShouldEqual, true)
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, 0)
		})
		Convey("Test_vector_string_all_null:", func() {
			s, err := db.RunScript("string(` ` ` )")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_any(t *testing.T) {
	Convey("Test_vector_any:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_any:", func() {
			s, err := db.RunScript("(1,'a',3,'','97c','2022.03.08')")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 25)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "any")
			result1 := s.(*model.Vector).SetNull
			So(result1, ShouldNotBeNil)
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, 0)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_char(t *testing.T) {
	Convey("Test_vector_char:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_char_not_null:", func() {
			s, err := db.RunScript("2c 98c 127c")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
			zx := [3]uint8{2, 98, 127}
			for i := 0; i < len(re); i++ {
				So(re[i], ShouldEqual, zx[i])
			}
			row := result.Rows()
			So(row, ShouldNotBeNil)
			idex := 2
			result.SetNull(idex)
			So(result.IsNull(idex), ShouldBeTrue)
		})
		Convey("Test_vector_char_has_null:", func() {
			s, err := db.RunScript("a = take(char[97c,,99c],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
			zx := [3]uint8{97, 0, 99}
			for i := 0; i < len(re); i++ {
				if i == 1 {
					So(result.IsNull(i), ShouldEqual, true)
				} else {
					So(re[i], ShouldEqual, zx[i])
				}
			}
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, -1)
		})
		Convey("Test_vector_char_all_null:", func() {
			s, err := db.RunScript("a= char(` ` ` );a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			for i := 0; i < len(re); i++ {
				So(result.IsNull(i), ShouldEqual, true)
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_bool(t *testing.T) {
	Convey("Test_vector_bool:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_bool_not_null:", func() {
			s, err := db.RunScript("true false true")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 1)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "bool")
			zx := [3]bool{true, false, true}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
		})
		Convey("Test_vector_bool_has_null:", func() {
			s, err := db.RunScript("a = take(bool[true,,false],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 1)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "bool")
			zx := [3]bool{true, false, false}
			var k int
			for i := 0; i < len(re); i++ {
				if i == 1 {
					So(result.IsNull(i), ShouldEqual, true)
					k++
				} else {
					if re[i] == zx[i] {
						k++
					}
				}
			}
			So(k, ShouldEqual, result.Data.Len())
		})
		Convey("Test_vector_bool_all_null:", func() {
			s, err := db.RunScript("take(00b, 3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			for i := 0; i < len(re); i++ {
				So(result.IsNull(i), ShouldEqual, true)
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 1)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "bool")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_symbol(t *testing.T) {
	Convey("Test_vector_symbol:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_symbol_not_null:", func() {
			s, err := db.RunScript("symbol(`a `b `c)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"a", "b", "c"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 17)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbol")
		})
		Convey("Test_vector_symbol_has_null:", func() {
			s, err := db.RunScript("a = take(symbol[`a ,,`c],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			reType := result.GetDataType()
			So(reType, ShouldEqual, 17)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbol")
			zx := [3]string{"a", "", "c"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
		})
		Convey("Test_vector_symbol_all_null:", func() {
			s, err := db.RunScript("a=symbol(` ` ` );a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 145)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "symbolExtend")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_int(t *testing.T) {
	Convey("Test_vector_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_int_not_null:", func() {
			s, err := db.RunScript("123 -321 1234")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]int32{123, -321, 1234}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, 0)
		})
		Convey("Test_vector_int_has_null:", func() {
			s, err := db.RunScript("a = take(int[123 ,, 1234],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]int32{123, model.NullInt, 1234}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		Convey("Test_vector_int_all_null:", func() {
			s, err := db.RunScript("take(00i, 3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == model.NullInt {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_short(t *testing.T) {
	Convey("Test_vector_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_short_not_null:", func() {
			s, err := db.RunScript("12h -32h 123h")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]int16{12, -32, 123}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
			get := result.Get(1).Value()
			if v, ok := get.(int); ok {
				So(v, ShouldEqual, -32)
			}
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, 0)
		})
		Convey("Test_vector_short_has_null:", func() {
			s, err := db.RunScript("a = take(short[-1258 ,, 17685],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]int16{-1258, 0, 17685}
			var k int
			for i := 0; i < len(re); i++ {
				if i == 1 {
					if result.IsNull(i) == true {
						k++
					}
				} else {
					if re[i] == zx[i] {
						k++
					}
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		Convey("Test_vector_short_all_null:", func() {
			s, err := db.RunScript("take(00h,3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_long(t *testing.T) {
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_long_not_null:", func() {
			s, err := db.RunScript("12l -32l 123l")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]int64{12, -32, 123}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
			asof := result.AsOf(result.Get(0))
			So(asof, ShouldEqual, 1)
		})
		Convey("Test_vector_long_has_null:", func() {
			s, err := db.RunScript("a = take(long[1048576 ,, 1048578],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]int64{1048576, model.NullLong, 1048578}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		Convey("Test_vector_long_all_null:", func() {
			s, err := db.RunScript("take(00l,3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == model.NullLong {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, -1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_double(t *testing.T) {
	Convey("Test_vector_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_double:", func() {
			s, err := db.RunScript("12.0 -32.0 123.0")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]float64{12.0, -32.0, 123.0}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		Convey("Test_vector_double_has_null:", func() {
			s, err := db.RunScript("a = take(double[1048576.0 ,, 1048578.0],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]float64{1048576, model.NullDouble, 1048578}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		Convey("Test_vector_double_all_null:", func() {
			s, err := db.RunScript("double(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == model.NullDouble {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_float(t *testing.T) {
	Convey("Test_vector_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_float_not_null:", func() {
			s, err := db.RunScript("12.5f -32.5f 123.5f")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]float32{12.5, -32.5, 123.5}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		Convey("Test_vector_float_has_null:", func() {
			s, err := db.RunScript("a = take(float[1048576.0 ,, 1048578.0],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]float32{1048576, model.NullFloat, 1048578}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		Convey("Test_vector_float_all_null:", func() {
			s, err := db.RunScript("take(00f, 3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == model.NullFloat {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_date(t *testing.T) {
	Convey("Test_vector_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_date_not_null:", func() {
			s, err := db.RunScript("1969.12.31 1970.01.01 1970.01.02 2006.01.02 2006.01.03 2022.08.03")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		Convey("Test_vector_date_has_null:", func() {
			s, err := db.RunScript("a = take(date[2022.07.29,,2022.07.31],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"2022-07-29 00:00:00", "", "2022-07-31 00:00:00"}
			t0, _ := time.Parse("2006-01-02 15:04:05", time1[0])
			t2, _ := time.Parse("2006-01-02 15:04:05", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		Convey("Test_vector_date_all_null:", func() {
			s, err := db.RunScript("take(00d, 3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_month(t *testing.T) {
	Convey("Test_vector_month:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_month_not_null:", func() {
			s, err := db.RunScript("1969.12M 1970.01M 1970.02M 2006.01M 2006.02M 2022.08M")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 2, 1, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 1, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_vector_month_has_null:", func() {
			s, err := db.RunScript("a = take(month[2022.07M,,2022.09M],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"2022-07", "", "2022-09"}
			t0, _ := time.Parse("2006-01", time1[0])
			t2, _ := time.Parse("2006-01", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_vector_month_all_null:", func() {
			s, err := db.RunScript("month(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_time(t *testing.T) {
	Convey("Test_vector_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_time_not_null:", func() {
			s, err := db.RunScript("23:59:59.999 00:00:00.000 00:00:01.999 15:04:04.999 15:04:05.000 15:00:15.000")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		Convey("Test_vector_time_has_null:", func() {
			s, err := db.RunScript("a = take(time[15:59:23.001,,15:59:23.003],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"1970-01-01T15:59:23.001", "", "1970-01-01T15:59:23.003"}
			t0, _ := time.Parse("2006-01-02T15:04:05.000", time1[0])
			t2, _ := time.Parse("2006-01-02T15:04:05.000", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		Convey("Test_vector_time_all_null:", func() {
			s, err := db.RunScript("time(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_minute(t *testing.T) {
	Convey("Test_vector_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_minute_not_null:", func() {
			s, err := db.RunScript("23:59m 00:00m 00:01m 15:04m 15:05m 15:15m")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 1, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 5, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 15, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		Convey("Test_vector_minute_has_null:", func() {
			s, err := db.RunScript("a = take(minute[15:56m,,15:58m],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"1970-01-01T15:56", "", "1970-01-01T15:58"}
			t0, _ := time.Parse("2006-01-02T15:04", time1[0])
			t2, _ := time.Parse("2006-01-02T15:04", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		Convey("Test_vector_minute_all_null:", func() {
			s, err := db.RunScript("take(00m, 3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_second(t *testing.T) {
	Convey("Test_vector_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_second_not_null:", func() {
			s, err := db.RunScript("23:59:59 00:00:00 00:00:01 15:04:04 15:04:05 15:00:15")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		Convey("Test_vector_second_has_null:", func() {
			s, err := db.RunScript("a = take(second[15:55:01,,15:55:03],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"1970-01-01T15:55:01", "", "1970-01-01T15:55:03"}
			t0, _ := time.Parse("2006-01-02T15:04:05", time1[0])
			t2, _ := time.Parse("2006-01-02T15:04:05", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		Convey("Test_vector_second_all_null:", func() {
			s, err := db.RunScript("take(00s, 3)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_datetime(t *testing.T) {
	Convey("Test_vector_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_datetime_not_null:", func() {
			s, err := db.RunScript("1969.12.31T23:59:59 1970.01.01T00:00:00 1970.01.01T00:00:01 2006.01.02T15:04:04 2006.01.02T15:04:05 2022.08.03T15:00:15")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		Convey("Test_vector_datetime_has_null:", func() {
			s, err := db.RunScript("a = take(datetime[2022.07.29 15:33:33,,2022.07.29 15:33:35],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"2022-07-29T15:33:33", "", "2022-07-29T15:33:35"}
			t0, _ := time.Parse("2006-01-02T15:04:05", time1[0])
			t2, _ := time.Parse("2006-01-02T15:04:05", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		Convey("Test_vector_datetime_all_null:", func() {
			s, err := db.RunScript("datetime(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_timestamp(t *testing.T) {
	Convey("Test_vector_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_timestamp_not_null:", func() {
			s, err := db.RunScript("1969.12.31T23:59:59.999 1970.01.01T00:00:00.000 1970.01.01T00:00:01.999 2006.01.02T15:04:04.999 2006.01.02T15:04:05.000 2022.08.03T15:00:15.000")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		Convey("Test_vector_timestamp_has_null:", func() {
			s, err := db.RunScript("a = take(timestamp[2022.07.29 15:00:04.201,,2022.07.29 15:00:04.203],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"2022-07-29T15:00:04.201", "", "2022-07-29T15:00:04.203"}
			t0, _ := time.Parse("2006-01-02T15:04:05", time1[0])
			t2, _ := time.Parse("2006-01-02T15:04:05", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		Convey("Test_vector_timestamp_all_null:", func() {
			s, err := db.RunScript("timestamp(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_nanotime(t *testing.T) {
	Convey("Test_vector_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_nanotime_not_null:", func() {
			s, err := db.RunScript("23:59:59.999999999 00:00:00.000000000 00:00:01.999999999 15:04:04.999999999 15:04:05.000000000 15:00:15.000000000")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 5, 0, time.UTC), time.Date(1970, 1, 1, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		Convey("Test_vector_nanotime_has_null:", func() {
			s, err := db.RunScript("a = take(nanotime[15:00:04.000000201,,15:00:04.000000203],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"1970-01-01 15:00:04.000000201", "", "1970-01-01 15:00:04.000000203"}
			t0, _ := time.Parse("2006-01-02 15:04:05.000000000", time1[0])
			t2, _ := time.Parse("2006-01-02 15:04:05.000000000", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		Convey("Test_vector_nanotime_all_null:", func() {
			s, err := db.RunScript("nanotime(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_nanotimestamp(t *testing.T) {
	Convey("Test_vector_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_nanotimestamp_not_null:", func() {
			s, err := db.RunScript("1969.12.31T23:59:59.999999999 1970.01.01T00:00:00.000000000 1970.01.01T00:00:01.999999999 2006.01.02T15:04:04.999999999 2006.01.02T15:04:05.000000000 2022.08.03T15:00:15.000000000")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC), time.Date(2022, 8, 3, 15, 0, 15, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		Convey("Test_vector_nanotimestamp_has_null:", func() {
			s, err := db.RunScript("a = take(nanotimestamp[2022.07.29 15:00:04.000000201,,2022.07.29 15:00:04.000000203],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"2022-07-29T15:00:04.000000201", "", "2022-07-29T15:00:04.000000203"}
			t0, _ := time.Parse("2006-01-02T15:04:05.000000000", time1[0])
			t2, _ := time.Parse("2006-01-02T15:04:05.000000000", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		Convey("Test_vector_nanotimestamp_all_null:", func() {
			s, err := db.RunScript("nanotimestamp(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_datehour(t *testing.T) {
	Convey("Test_vector_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_datehour_not_null:", func() {
			s, err := db.RunScript("datehour[1969.12.31T23:59:59.999, 1970.01.01T00:00:00.000, 2006.01.02T15:04:04.999]")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Rows())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		Convey("Test_vector_datehour_has_null:", func() {
			s, err := db.RunScript("a = take(datehour[2022.07.29 15:00:00.000,,2022.07.29 17:00:00.000],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			time1 := [3]string{"2022-07-29T15", "", "2022-07-29T17"}
			t0, _ := time.Parse("2006-01-02T15", time1[0])
			t2, _ := time.Parse("2006-01-02T15", time1[2])
			So(re[0], ShouldEqual, t0)
			So(re[2], ShouldEqual, t2)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		Convey("Test_vector_datehour_all_null:", func() {
			s, err := db.RunScript("datehour(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_uuid(t *testing.T) {
	Convey("Test_vector_uuid:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_uuid_not_null:", func() {
			s, err := db.RunScript("uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', '5d212a78-cc48-e3b1-4235-b4d91473ee89'])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
		})
		Convey("Test_vector_uuid_has_null:", func() {
			s, err := db.RunScript("a = take(uuid['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', '5d212a78-cc48-e3b1-4235-b4d91473ee89'],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			var k int
			for i := 0; i < len(re); i++ {
				if i == 1 {
					if result.IsNull(i) == true {
						k++
					}
				} else {
					if re[i] == zx[i] {
						k++
					}
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
		})
		Convey("Test_vector_uuid_all_null:", func() {
			s, err := db.RunScript("uuid(['','',''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			for i := 0; i < len(re); i++ {
				So(result.IsNull(i), ShouldEqual, true)
			}
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_ipaddr(t *testing.T) {
	Convey("Test_vector_ipaddr:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_ipaddr_not_null:", func() {
			s, err := db.RunScript("ipaddr(['461c:7fa1:7f3c:7249:5278:c610:f595:d174', '3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72', '127e:eeed:1b16:20a9:1694:6185:f045:fb9a'])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"461c:7fa1:7f3c:7249:5278:c610:f595:d174", "3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72", "127e:eeed:1b16:20a9:1694:6185:f045:fb9a"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		Convey("Test_vector_ipaddr_number_has_null:", func() {
			s, err := db.RunScript("a = take(ipaddr['192.168.1.135', , '192.168.1.14'],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"192.168.1.135", "0.0.0.0", "192.168.1.14"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		Convey("Test_vector_ipaddr_all_null:", func() {
			s, err := db.RunScript("ipaddr(['', '', ''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		Convey("Test_vector_ipaddr_number_not_null:", func() {
			s, err := db.RunScript("ipaddr(['192.168.1.135', '192.168.1.124', '192.168.1.14'])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"192.168.1.135", "192.168.1.124", "192.168.1.14"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_int128(t *testing.T) {
	Convey("Test_vector_int128:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_int128_not_null:", func() {
			s, err := db.RunScript("int128(['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', 'e1671797c52e15f763380b45e841ec34'])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
		})
		Convey("Test_vector_int128_has_null:", func() {
			s, err := db.RunScript("a = take(int128['e1671797c52e15f763380b45e841ec32', , 'e1671797c52e15f763380b45e841ec34'],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000", "e1671797c52e15f763380b45e841ec34"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
		})
		Convey("Test_vector_int128_all_null:", func() {
			s, err := db.RunScript("int128(['', '', ''])")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			var k int
			for i := 0; i < len(re); i++ {
				if result.IsNull(i) == true {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
			hush := result.HashBucket(1, 1)
			So(hush, ShouldEqual, 0)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_complex(t *testing.T) {
	Convey("Test_vector_complex:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_complex_not_null:", func() {
			s, err := db.RunScript("a = take([complex(2,5),complex(-2,-5),complex(1048576,1048578)],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"2.00000+5.00000i", "-2.00000+-5.00000i", "1048576.00000+1048578.00000i"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 34)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "complex")
		})
		Convey("Test_vector_complex_has_null:", func() {
			s, err := db.RunScript("a = take([complex(-2,5),,complex(-1048576,-1048578)],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"-2.00000+5.00000i", "0.00000+0.00000i", "-1048576.00000+-1048578.00000i"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 34)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "complex")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_point(t *testing.T) {
	Convey("Test_vector_point:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_point:", func() {
			s, err := db.RunScript("a = take([point(2,5),point(-2,-5),point(1048576,1048578)],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"(2.00000, 5.00000)", "(-2.00000, -5.00000)", "(1048576.00000, 1048578.00000)"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 35)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "point")
		})
		Convey("Test_vector_point_has_null:", func() {
			s, err := db.RunScript("a = take([point(-2,5),,point(-1048576,-1048578)],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			zx := [3]string{"(-2.00000, 5.00000)", "(0.00000, 0.00000)", "(-1048576.00000, -1048578.00000)"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, result.Data.Len())
			reType := result.GetDataType()
			So(reType, ShouldEqual, 35)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "point")
			So(result.HashBucket(1, 1), ShouldEqual, -1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_duration(t *testing.T) {
	Convey("Test_vector_duration:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_durations_not_null:", func() {
			s, err := db.RunScript("a = take(duration['1H'],3);a")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.Data.Value()
			So(re, ShouldNotBeNil)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 25)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "any")
			str := result.String()
			So(str, ShouldEqual, "vector<any>([duration(1H), duration(1H), duration(1H)])")
		})
	})
}
func Test_Vector_Download_Datatype_vector_big_than_1024(t *testing.T) {
	Convey("Test_vector_big_than_1024:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a=take(10000.0+1..16,2048);a.append!(-1024.0);a")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		re := result.Data.Value()
		So(re[0], ShouldEqual, 10001.0)
		So(re[2047], ShouldEqual, 10016.0)
		So(re[2048], ShouldEqual, -1024.0)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtDouble)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "double")
		So(result.ColumnCount, ShouldEqual, 1)
		So(result.RowCount, ShouldEqual, 2049)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_vector_big_than_1048576(t *testing.T) {
	Convey("Test_vector_big_than_1048576:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a=take(datetime(2022.01.03T12:59:59.000)+1..1024,1048576);a.append!(datetime(2022.09.02T23:59:59.000));a")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		re := result.Data.Value()
		ex1 := time.Date(2022, 1, 3, 13, 0, 0, 0, time.UTC)
		ex2 := time.Date(2022, 1, 3, 13, 17, 3, 0, time.UTC)
		ex3 := time.Date(2022, 9, 2, 23, 59, 59, 0, time.UTC)
		So(re[0], ShouldEqual, ex1)
		So(re[1048575], ShouldEqual, ex2)
		So(re[1048576], ShouldEqual, ex3)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtDatetime)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "datetime")
		So(result.ColumnCount, ShouldEqual, 1)
		So(result.RowCount, ShouldEqual, 1048577)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_array_vector(t *testing.T) {
	Convey("Test_vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a = array(INT[],0).append!([[1,1],[2,2],[3,3],[4,4],[5,5],[6]]);a")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		re := result.GetVectorValue(1).Data.Value()
		So(re[0], ShouldEqual, 2)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtInt+64)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "intArray")
		So(result.ColumnCount, ShouldEqual, 11)
		So(result.RowCount, ShouldEqual, 6)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_array_vector_empty(t *testing.T) {
	Convey("Test_vector_array_vector_empty:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a = array(UUID[],0);a")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtUUID+64)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "uuidArray")
		So(result.RowCount, ShouldEqual, 0)
	})
}
func Test_Vector_Download_Datatype_array_vector_big_than_1024(t *testing.T) {
	Convey("Test_vector_array_vector_big_than_1024:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a = array(DOUBLE[],0).append!(take([[1.0,1.025],[2.0,2.36954],[3.32665,3.3266],[4.115,412.15],[5.215,5.545],[6.16546],[12.7,8.2,1.9,7.36,8.65,9.96],[-123.123,-258.258,-456.369]],2048)).append!(-1024.123456);a")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		So(result.GetVectorValue(1).Data.Value()[1], ShouldEqual, 2.36954)
		So(result.GetVectorValue(2047).Data.Value()[0], ShouldEqual, -123.123)
		So(result.GetVectorValue(2048).Data.Value()[0], ShouldEqual, -1024.123456)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtDouble+64)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "doubleArray")
		So(result.RowCount, ShouldEqual, 2049)
	})
}
func Test_Vector_Download_Datatype_array_vector_big_than_1048576(t *testing.T) {
	Convey("Test_vector_array_vector_big_than_1048576:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("a = array(DATETIME[],0).append!(take([[1996.12.31T23:59:59,1997.01.01T00:00:00],[2006.01.02T15:04:04,2006.01.02T15:04:05],[2022.01.02T23:59:59,2006.01.02T15:04:59],[2022.09.02T15:04:04,3002.01.02T15:04:05]],1048576)).append!(2002.02.02T12:24:36);a")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		So(result.GetVectorValue(1).Data.Value()[1], ShouldEqual, time.Date(2006, 1, 2, 15, 4, 5, 0, time.UTC))
		So(result.GetVectorValue(1048575).Data.Value()[0], ShouldEqual, time.Date(2022, 9, 2, 15, 4, 4, 0, time.UTC))
		So(result.GetVectorValue(1048576).Data.Value()[0], ShouldEqual, time.Date(2002, 2, 2, 12, 24, 36, 0, time.UTC))
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtDatetime+64)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "datetimeArray")
		So(result.RowCount, ShouldEqual, 1048577)
	})
}
func Test_Vector_Download_Datatype_bigArray(t *testing.T) {
	Convey("Test_vector_bigArray:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n=5000000;X=bigarray(long,0, n);X.append!(1..4999999);X.append!(2000000000l);X")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		re := result.Data.Value()
		So(re[4999999], ShouldEqual, 2000000000)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtLong)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "long")
		So(result.ColumnCount, ShouldEqual, 1)
		So(result.RowCount, ShouldEqual, 5000000)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_subArray(t *testing.T) {
	Convey("Test_vector_subArray:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s, err := db.RunScript("n=5000000;X=bigarray(long,0, n);X.append!(1..4999999);X.append!(2000000000l);Y=subarray(X,4999998:);Y")
		So(err, ShouldBeNil)
		result := s.(*model.Vector)
		re := result.Data.Value()
		So(re[0], ShouldEqual, 4999999)
		So(re[1], ShouldEqual, 2000000000)
		reType := result.GetDataType()
		So(reType, ShouldEqual, model.DtLong)
		reTypeString := result.GetDataTypeString()
		So(reTypeString, ShouldEqual, "long")
		So(result.ColumnCount, ShouldEqual, 1)
		So(result.RowCount, ShouldEqual, 2)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_int(t *testing.T) {
	Convey("Test_vector_int_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_int:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtInt, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST INT VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtInt)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_short(t *testing.T) {
	Convey("Test_vector_short_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_short:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtShort, []int16{1, 2, 3, 4, 5, 6, 7, 8, 9})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []int16{1, 2, 3, 4, 5, 6, 7, 8, 9}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST SHORT VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtShort)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_char(t *testing.T) {
	Convey("Test_vector_char_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_char:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtChar, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST CHAR VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtChar)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_long(t *testing.T) {
	Convey("Test_vector_long_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_long:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtLong, []int64{-1, -2, -3, 4, 5, 6, 7, 8, 9})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []int64{-1, -2, -3, 4, 5, 6, 7, 8, 9}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST LONG VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtLong)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_float(t *testing.T) {
	Convey("Test_vector_float_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_float:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtFloat, []float32{-1, -2, -3, 4, 5, 6, 7, 8, 9})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []float32{-1, -2, -3, 4, 5, 6, 7, 8, 9}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST FLOAT VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtFloat)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_double(t *testing.T) {
	Convey("Test_vector_double_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_double:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtDouble, []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, -10247.36985, 8, 9})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, -10247.36985, 8, 9}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DOUBLE VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtDouble)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_date(t *testing.T) {
	Convey("Test_vector_date_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_date:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DATE VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtDate)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_month(t *testing.T) {
	Convey("Test_vector_month_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_month:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST MONTH VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtMonth)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_time(t *testing.T) {
	Convey("Test_vector_time_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_time:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST TIME VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_minute(t *testing.T) {
	Convey("Test_vector_minute_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_minute:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST MINUTE VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtMinute)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_second(t *testing.T) {
	Convey("Test_vector_second_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_second:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST SECOND VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtSecond)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_datetime(t *testing.T) {
	Convey("Test_vector_datetime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_datetime:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DATETIME VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtDatetime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_timestamp(t *testing.T) {
	Convey("Test_vector_timestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_timestamp:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST TIMESTAMP VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_nanotime(t *testing.T) {
	Convey("Test_vector_nanotime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_nanotime:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST NANOTIME VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_nanotimestamp(t *testing.T) {
	Convey("Test_vector_nanotimestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_nanotimestamp:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST NANOTIMESTAMP VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_datehour(t *testing.T) {
	Convey("Test_vector_datehour_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_datehour:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == time1[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST DATEHOUR VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtDateHour)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_point(t *testing.T) {
	Convey("Test_vector_point_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_point:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtPoint, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []string{"(1.00000, 1.00000)", "(-1.00000, -1024.50000)", "(1001022.40000, -30028.75000)"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST POINT VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtPoint)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_complex(t *testing.T) {
	Convey("Test_vector_complex_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_complex:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtComplex, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []string{"1.00000+1.00000i", "-1.00000+-1024.50000i", "1001022.40000+-30028.75000i"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST COMPLEX VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtComplex)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_string(t *testing.T) {
	Convey("Test_vector_string_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_string:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtString, []string{"hello", "#$%", "数据类型", "what"})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []string{"hello", "#$%", "数据类型", "what"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(STRING VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtString)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_any(t *testing.T) {
	Convey("Test_vector_any_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_any:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtAny, model.DfVector)
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(ANY VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtAny)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_bool(t *testing.T) {
	Convey("Test_vector_bool_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_bool:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtBool, []bool{true, true, false, false})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []bool{true, true, false, false}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST BOOL VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtBool)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_blob(t *testing.T) {
	Convey("Test_vector_blob_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_blob:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtBlob, [][]byte{{6}, {12}, {56}, {128}})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := [][]uint8{{6}, {12}, {56}, {128}}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldResemble, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(BLOB VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtBlob)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_uuid(t *testing.T) {
	Convey("Test_vector_uuid_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_uuid:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST UUID VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtUUID)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_ipaddr(t *testing.T) {
	Convey("Test_vector_ipaddr_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_ipaddr:", func() {
			dls, _ := model.NewDataTypeListWithRaw(model.DtIP, []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Vector).Data.Value()
			zx := []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"}
			var k int
			for i := 0; i < len(re); i++ {
				if re[i] == zx[i] {
					k++
				}
			}
			So(k, ShouldEqual, res.Rows())
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FAST IPADDR VECTOR)")
			So(res.GetDataType(), ShouldEqual, model.DtIP)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_int28(t *testing.T) {
	Convey("Test_vector_int128_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		dls, _ := model.NewDataTypeListWithRaw(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"})
		s := model.NewVector(dls)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		res, _ := db.RunScript("s")
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Vector).Data.Value()
		zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"}
		var k int
		for i := 0; i < len(re); i++ {
			if re[i] == zx[i] {
				k++
			}
		}
		So(k, ShouldEqual, res.Rows())
		So(err, ShouldBeNil)
		So(ty.String(), ShouldEqual, "string(FAST INT128 VECTOR)")
		So(res.GetDataType(), ShouldEqual, model.DtInt128)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_int_array_vector(t *testing.T) {
	Convey("Test_Vector_int_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		int1v, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{-1024, 1048576, -1048579, 3000000})
		So(err, ShouldBeNil)
		int2v, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{0, 1048576, model.NullInt, 3000000})
		So(err, ShouldBeNil)
		int3v, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{model.NullInt, model.NullInt, model.NullInt, model.NullInt})
		So(err, ShouldBeNil)
		av := model.NewArrayVector([]*model.Vector{model.NewVector(int1v), model.NewVector(int2v), model.NewVector(int3v)})
		s := model.NewVectorWithArrayVector(av)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(err, ShouldBeNil)
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Vector)
		So(re.Get(5).Value(), ShouldEqual, 1048576)
		So(re.IsNull(6), ShouldBeTrue)
		So(ty.String(), ShouldEqual, "string(FAST INT[] VECTOR)")
		So(res.GetDataType(), ShouldEqual, model.DtInt+64)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_bool_array_vector(t *testing.T) {
	Convey("Test_Vector_bool_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		bool1v, err := model.NewDataTypeListWithRaw(model.DtBool, []byte{1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
		So(err, ShouldBeNil)
		bool2v, err := model.NewDataTypeListWithRaw(model.DtBool, []byte{0, model.NullBool, 1})
		So(err, ShouldBeNil)
		bool3v, err := model.NewDataTypeListWithRaw(model.DtBool, []byte{model.NullBool, model.NullBool, model.NullBool})
		So(err, ShouldBeNil)
		av := model.NewArrayVector([]*model.Vector{model.NewVector(bool1v), model.NewVector(bool2v), model.NewVector(bool3v)})
		s := model.NewVectorWithArrayVector(av)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(err, ShouldBeNil)
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Vector)
		So(re.Get(0).Value(), ShouldEqual, true)
		So(re.Get(6).Value(), ShouldEqual, false)
		So(re.IsNull(12), ShouldBeTrue)
		So(ty.String(), ShouldEqual, "string(FAST BOOL[] VECTOR)")
		So(res.GetDataType(), ShouldEqual, model.DtBool+64)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_big_array_vector(t *testing.T) {
	Convey("Test_Vector_int_big_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		var i int32
		sz := []int32{}
		for i = 0; i < 1048579*12; i += 12 {
			sz = append(sz, i)
		}
		int1v, err := model.NewDataTypeListWithRaw(model.DtInt, sz)
		So(err, ShouldBeNil)
		int2v, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{0, 1048576, model.NullInt, 3000000})
		So(err, ShouldBeNil)
		int3v, err := model.NewDataTypeListWithRaw(model.DtInt, []int32{model.NullInt, model.NullInt, model.NullInt, model.NullInt})
		So(err, ShouldBeNil)
		av := model.NewArrayVector([]*model.Vector{model.NewVector(int1v), model.NewVector(int2v), model.NewVector(int3v)})
		s := model.NewVectorWithArrayVector(av)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(err, ShouldBeNil)
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Vector)
		So(re.ColumnCount, ShouldEqual, 1048587)
		So(re.Get(5).Value(), ShouldEqual, 5*12)
		So(re.Get(995).Value(), ShouldEqual, 995*12)
		So(re.Get(133546).Value(), ShouldEqual, 133546*12)
		So(re.IsNull(1048579+2), ShouldBeTrue)
		So(ty.String(), ShouldEqual, "string(FAST INT[] VECTOR)")
		So(res.GetDataType(), ShouldEqual, model.DtInt+64)
		So(db.Close(), ShouldBeNil)
	})
}
