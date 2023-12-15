package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func Test_Vector_Download_Datatype_string(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
			So(reTypeString, ShouldEqual, "datehour")
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
			So(reTypeString, ShouldEqual, "datehour")
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
			So(reTypeString, ShouldEqual, "datehour")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_decimal32(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_decimal32:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_decimal32_not_null:", func() {
			s, err := db.RunScript("decimal32([-3.1235565,1.1,0], 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "-3.12")
			So(result.Get(1).String(), ShouldEqual, "1.10")
			So(result.Get(2).String(), ShouldEqual, "0.00")

			re := result.Data.Value()
			So(re[0].(*model.Decimal32).Scale, ShouldEqual, 2)
			So(re[1].(*model.Decimal32).Scale, ShouldEqual, 2)
			So(re[2].(*model.Decimal32).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, 37)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal32")
		})
		Convey("Test_vector_decimal32_has_null:", func() {
			s, err := db.RunScript("decimal32([-3.1235565, 1.1, NULL], 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "-3.12")
			So(result.Get(1).String(), ShouldEqual, "1.10")
			So(result.Get(2).String(), ShouldEqual, "")

			re := result.Data.Value()
			So(re[0].(*model.Decimal32).Scale, ShouldEqual, 2)
			So(re[1].(*model.Decimal32).Scale, ShouldEqual, 2)
			So(re[2].(*model.Decimal32).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, 37)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal32")
		})
		Convey("Test_vector_decimal32_all_null:", func() {
			s, err := db.RunScript("[decimal32(NULL, 2)]")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "")

			re := result.Data.Value()
			So(re[0].(*model.Decimal32).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, 37)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal32")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_decimal64(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_decimal64:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_decimal64_not_null:", func() {
			s, err := db.RunScript("decimal64([-3.1235565,1.1,0], 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "-3.12")
			So(result.Get(1).String(), ShouldEqual, "1.10")
			So(result.Get(2).String(), ShouldEqual, "0.00")

			re := result.Data.Value()
			So(re[0].(*model.Decimal64).Scale, ShouldEqual, 2)
			So(re[1].(*model.Decimal64).Scale, ShouldEqual, 2)
			So(re[2].(*model.Decimal64).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, 38)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal64")
		})
		Convey("Test_vector_decimal64_has_null:", func() {
			s, err := db.RunScript("decimal64([-3.1235565, 1.1, NULL], 2)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "-3.12")
			So(result.Get(1).String(), ShouldEqual, "1.10")
			So(result.Get(2).String(), ShouldEqual, "")

			re := result.Data.Value()
			So(re[0].(*model.Decimal64).Scale, ShouldEqual, 2)
			So(re[1].(*model.Decimal64).Scale, ShouldEqual, 2)
			So(re[2].(*model.Decimal64).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, 38)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal64")
		})
		Convey("Test_vector_decimal64_all_null:", func() {
			s, err := db.RunScript("[decimal64(NULL, 2)]")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "")

			re := result.Data.Value()
			So(re[0].(*model.Decimal64).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, 38)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal64")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_decimal128(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_decimal128:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_decimal128_not_null:", func() {
			s, err := db.RunScript("decimal128([`0, '-1.123123123123123123123123123123123123123', '987654321.123456789'], 26)")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "0.00000000000000000000000000")
			So(result.Get(1).String(), ShouldEqual, "-1.12312312312312312312312312")
			So(result.Get(2).String(), ShouldEqual, "987654321.12345678900000000000000000")

			re := result.Data.Value()
			So(re[0].(*model.Decimal128).Scale, ShouldEqual, 26)
			So(re[1].(*model.Decimal128).Scale, ShouldEqual, 26)
			So(re[2].(*model.Decimal128).Scale, ShouldEqual, 26)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, model.DtDecimal128)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal128")
		})
		Convey("Test_vector_decimal128_has_null:", func() {
			s, err := db.RunScript(`decimal128(["-1.2", "", NULL], 2)`)
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "-1.20")
			So(result.Get(1).IsNull(), ShouldBeTrue)
			So(result.Get(2).IsNull(), ShouldBeTrue)

			re := result.Data.Value()
			So(re[0].(*model.Decimal128).Scale, ShouldEqual, 2)
			So(re[1].(*model.Decimal128).Scale, ShouldEqual, 2)
			So(re[2].(*model.Decimal128).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, model.DtDecimal128)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal128")
		})
		Convey("Test_vector_decimal128_all_null:", func() {
			s, err := db.RunScript("[decimal128(NULL, 2)]")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			So(result.Get(0).String(), ShouldEqual, "")

			re := result.Data.Value()
			So(re[0].(*model.Decimal128).Scale, ShouldEqual, 2)

			reType := result.GetDataType()
			reForm := result.GetDataForm()
			So(reForm, ShouldEqual, model.DfVector)
			So(reType, ShouldEqual, model.DtDecimal128)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal128")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_uuid(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
			So(reTypeString, ShouldEqual, "ipaddr")
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
			So(reTypeString, ShouldEqual, "ipaddr")
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
			So(reTypeString, ShouldEqual, "ipaddr")
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
			So(reTypeString, ShouldEqual, "ipaddr")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_Download_Datatype_int128(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
			zx := [3]string{"-2.00000+5.00000i", "", "-1048576.00000+-1048578.00000i"}
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
	t.Parallel()
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
			zx := [3]string{"(-2.00000, 5.00000)", "(,)", "(-1048576.00000, -1048578.00000)"}
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	Convey("Test_vector_int_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_int:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9})
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
	t.Parallel()
	Convey("Test_vector_short_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_short:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtShort, []int16{1, 2, 3, 4, 5, 6, 7, 8, 9})
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
	t.Parallel()
	Convey("Test_vector_char_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_char:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtChar, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
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
	t.Parallel()
	Convey("Test_vector_long_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_long:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtLong, []int64{-1, -2, -3, 4, 5, 6, 7, 8, 9})
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
	t.Parallel()
	Convey("Test_vector_float_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_float:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtFloat, []float32{-1, -2, -3, 4, 5, 6, 7, 8, 9})
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
	t.Parallel()
	Convey("Test_vector_double_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_double:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDouble, []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, -10247.36985, 8, 9})
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
	t.Parallel()
	Convey("Test_vector_date_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_date:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_month_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_month:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_time_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_time:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_minute_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_minute:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_second_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_second:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_datetime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_datetime:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_timestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_timestamp:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_nanotime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_nanotime:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_nanotimestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_nanotimestamp:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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
	t.Parallel()
	Convey("Test_vector_datehour_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_datehour:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
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

func Test_Vector_UpLoad_Datatype_decimal32(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_decimal32_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_decimal32:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 2, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("ex=decimal32([3.21235, -1, NULL], 2);eqObj(s, ex)")
			So(res.(*model.Scalar).Value(), ShouldBeTrue)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_decimal64(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_decimal64_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_decimal64:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 2, Value: []float64{3.21235, -1, model.NullDecimal64Value}})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("ex=decimal64([3.21235, -1, NULL], 2);eqObj(s, ex)")
			So(res.(*model.Scalar).Value(), ShouldBeTrue)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_Datatype_decimal128(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_decimal128_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_decimal128:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 0, Value: []string{"0", "-1.123123123123123123123123123123123123123", "987654321.123456789", model.NullDecimal128Value, ""}})
			s := model.NewVector(dls)
			_, err := db.Upload(map[string]model.DataForm{"s": s})
			So(err, ShouldBeNil)
			res, _ := db.RunScript("ex=decimal128([`0, '-1', `987654321, NULL, string(NULL)], 0);eqObj(s, ex)")
			So(res.(*model.Scalar).Value(), ShouldBeTrue)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_Datatype_point(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_point_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_point:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtPoint, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
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
	t.Parallel()
	Convey("Test_vector_complex_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_complex:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtComplex, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
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
	t.Parallel()
	Convey("Test_vector_string_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_string:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtString, []string{"hello", "#$%", "数据类型", "what"})
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
	t.Parallel()
	Convey("Test_vector_any_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_any:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtAny, model.DfVector)
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
	t.Parallel()
	Convey("Test_vector_bool_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_bool:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtBool, []bool{true, true, false, false})
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
	t.Parallel()
	Convey("Test_vector_blob_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_blob:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtBlob, [][]byte{{6}, {12}, {56}, {128}})
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
	t.Parallel()
	Convey("Test_vector_uuid_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_uuid:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"})
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
	t.Parallel()
	Convey("Test_vector_ipaddr_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_ipaddr:", func() {
			dls, _ := model.NewDataTypeListFromRawData(model.DtIP, []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"})
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
	t.Parallel()
	Convey("Test_vector_int128_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		dls, _ := model.NewDataTypeListFromRawData(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"})
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
	t.Parallel()
	Convey("Test_Vector_int_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		int1v, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{-1024, 1048576, -1048579, 3000000})
		So(err, ShouldBeNil)
		int2v, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{0, 1048576, model.NullInt, 3000000})
		So(err, ShouldBeNil)
		int3v, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{model.NullInt, model.NullInt, model.NullInt, model.NullInt})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(int1v), model.NewVector(int2v), model.NewVector(int3v)})
		s := model.NewVectorWithArrayVector(NewData)
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
	t.Parallel()
	Convey("Test_Vector_bool_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		bool1v, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{1, 1, 0, 1, 1, 0, 0, 0, 1, 0, 1})
		So(err, ShouldBeNil)
		bool2v, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{0, model.NullBool, 1})
		So(err, ShouldBeNil)
		bool3v, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{model.NullBool, model.NullBool, model.NullBool})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(bool1v), model.NewVector(bool2v), model.NewVector(bool3v)})
		s := model.NewVectorWithArrayVector(NewData)
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
	t.Parallel()
	Convey("Test_Vector_int_big_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		var i int32
		sz := []int32{}
		for i = 0; i < 1048579*12; i += 12 {
			sz = append(sz, i)
		}
		int1v, err := model.NewDataTypeListFromRawData(model.DtInt, sz)
		So(err, ShouldBeNil)
		int2v, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{0, 1048576, model.NullInt, 3000000})
		So(err, ShouldBeNil)
		int3v, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{model.NullInt, model.NullInt, model.NullInt, model.NullInt})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(int1v), model.NewVector(int2v), model.NewVector(int3v)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(err, ShouldBeNil)
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Vector)
		re.GetVectorValue(0)
		re.GetVectorValue(1)
		re.GetVectorValue(2)
		So(re.GetVectorValue(0).Rows(), ShouldEqual, 1048579)
		So(re.GetVectorValue(1).Rows(), ShouldEqual, 4)
		So(re.GetVectorValue(2).Rows(), ShouldEqual, 4)
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

func Test_arrayvector_Download_Datatype_int(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(INT[], 0, 10).append!([1..10]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 68)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "intArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(INT[], 0, 10).append!([1 NULL 3]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<int>([1, , 3])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 68)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "intArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, 1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, 3)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(INT[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<int>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 68)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "intArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_long(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(LONG[], 0, 10).append!([1..10]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<long>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 69)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "longArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(LONG[], 0, 10).append!([1 NULL 3]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<long>([1, , 3])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 69)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "longArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, 1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, 3)
		})

		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(LONG[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<long>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 69)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "longArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_short(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(SHORT[], 0, 10).append!([1..10]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<short>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 67)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "shortArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(SHORT[], 0, 10).append!([1 NULL 3]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<short>([1, , 3])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 67)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "shortArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, 1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, 3)
		})

		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(SHORT[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<short>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 67)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "shortArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_double(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(DOUBLE[], 0, 10).append!([1..10]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<double>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 80)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "doubleArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(DOUBLE[], 0, 10).append!([1 NULL 3]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<double>([1, , 3])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 80)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "doubleArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, 1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, 3)
		})

		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(DOUBLE[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<double>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 80)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "doubleArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_float(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(FLOAT[], 0, 10).append!([1..10]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<float>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 79)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "floatArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(FLOAT[], 0, 10).append!([1 NULL 3]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<float>([1, , 3])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 79)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "floatArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, 1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, 3)
		})

		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(FLOAT[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<float>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 79)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "floatArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_bool(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(BOOL[], 0, 10).append!([[true, false, true]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<bool>([true, false, true])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 65)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "boolArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(BOOL[], 0, 10).append!([true NULL false]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<bool>([true, , false])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 65)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "boolArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, false)
		})

		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(BOOL[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<bool>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 65)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "boolArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_char(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(CHAR[], 0, 10).append!([[1, 2, 3]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<char>([1, 2, 3])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 66)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "charArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(CHAR[], 0, 10).append!([1 NULL 4]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<char>([1, , 4])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 66)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "charArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).Value(), ShouldEqual, 1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, 4)
		})

		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(CHAR[], 0, 10).append!([take(00i, 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<char>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 66)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "charArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_date(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(DATE[], 0, 10).append!([[1969.12.31, 1970.01.01, 1972.12.03]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<date>([1969.12.31, 1970.01.01, 1972.12.03])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 70)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(DATE[], 0, 10).append!([[1970.01.01, NULL, 1969.12.03]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<date>([1970.01.01, , 1969.12.03])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 70)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timestamp1 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			timestamp2 := time.Date(1969, 12, 03, 0, 0, 0, 0, time.UTC)
			So(re.Get(0).Value(), ShouldEqual, timestamp1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, timestamp2)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(DATE[], 0, 10).append!([take(date(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<date>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 70)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_month(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(MONTH[], 0, 10).append!([[1969.03M, 1970.01M, 1972.12M]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<month>([1969.03M, 1970.01M, 1972.12M])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 71)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "monthArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(MONTH[], 0, 10).append!([[1970.01M, NULL, 1969.12M]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<month>([1970.01M, , 1969.12M])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 71)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "monthArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timestamp1 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
			timestamp2 := time.Date(1969, 12, 01, 0, 0, 0, 0, time.UTC)
			So(re.Get(0).Value(), ShouldEqual, timestamp1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, timestamp2)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(MONTH[], 0, 10).append!([take(month(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<month>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 71)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "monthArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_time(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(TIME[], 0, 10).append!([[00:00:00.001, 00:00:00.003, 00:00:00.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<time>([00:00:00.001, 00:00:00.003, 00:00:00.100])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 72)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(TIME[], 0, 10).append!([[00:00:00.001, NULL, 00:00:00.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<time>([00:00:00.001, , 00:00:00.100])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 72)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timex := [3]string{"1970-01-01T00:00:00.001", "", "1970-01-01T00:00:00.100"}
			m, _ := time.Parse("2006-01-02T15:04:05.000", timex[0])
			n, _ := time.Parse("2006-01-02T15:04:05.000", timex[2])
			So(re.Get(0).Value(), ShouldEqual, m)
			So(re.Get(2).Value(), ShouldEqual, n)
			So(re.Get(1).IsNull(), ShouldEqual, true)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(TIME[], 0, 10).append!([take(time(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<time>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 72)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_minute(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(MINUTE[], 0, 10).append!([[12:12:14.001, 13:24:59.003, 12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<minute>([12:12m, 13:24m, 12:45m])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 73)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minuteArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(MINUTE[], 0, 10).append!([[12:12:14.001, NULL, 12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<minute>([12:12m, , 12:45m])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 73)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minuteArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timex := [3]string{"1970-01-01T12:12:00.000", "", "1970-01-01T12:45:00.000"}
			m, _ := time.Parse("2006-01-02T15:04:05.000", timex[0])
			n, _ := time.Parse("2006-01-02T15:04:05.000", timex[2])
			So(re.Get(0).Value(), ShouldEqual, m)
			So(re.Get(2).Value(), ShouldEqual, n)
			So(re.Get(1).IsNull(), ShouldEqual, true)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(MINUTE[], 0, 10).append!([take(minute(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<minute>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 73)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minuteArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_second(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(SECOND[], 0, 10).append!([[12:12:14.001, 13:24:59.003, 12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<second>([12:12:14, 13:24:59, 12:45:59])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 74)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "secondArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(SECOND[], 0, 10).append!([[12:12:14.001, NULL, 12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<second>([12:12:14, , 12:45:59])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 74)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "secondArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timex := [3]string{"1970-01-01T12:12:14.000", "", "1970-01-01T12:45:59.000"}
			m, _ := time.Parse("2006-01-02T15:04:05.000", timex[0])
			n, _ := time.Parse("2006-01-02T15:04:05.000", timex[2])
			So(re.Get(0).Value(), ShouldEqual, m)
			So(re.Get(2).Value(), ShouldEqual, n)
			So(re.Get(1).IsNull(), ShouldEqual, true)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(SECOND[], 0, 10).append!([take(second(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<second>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 74)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "secondArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_datetime(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(DATETIME[], 0, 10).append!([[2012.01.01T12:12:14.001, 2012.01.01T13:24:59.003, 2012.01.01T12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<datetime>([2012.01.01T12:12:14, 2012.01.01T13:24:59, 2012.01.01T12:45:59])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 75)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetimeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(DATETIME[], 0, 10).append!([[1969.01.01T12:12:14.001, NULL, 1970.12.13T12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<datetime>([1969.01.01T12:12:14, , 1970.12.13T12:45:59])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 75)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetimeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timestamp1 := time.Date(1969, 1, 1, 12, 12, 14, 0, time.UTC)
			timestamp2 := time.Date(1970, 12, 13, 12, 45, 59, 0, time.UTC)
			So(re.Get(0).Value(), ShouldEqual, timestamp1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, timestamp2)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(DATETIME[], 0, 10).append!([take(datetime(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<datetime>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 75)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetimeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_timestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(TIMESTAMP[], 0, 10).append!([[2012.01.01T12:12:14.001, 2012.01.01T13:24:59.003, 2012.01.01T12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<timestamp>([2012.01.01T12:12:14.001, 2012.01.01T13:24:59.003, 2012.01.01T12:45:59.100])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 76)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestampArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(TIMESTAMP[], 0, 10).append!([[1969.01.01T12:12:14.123, NULL, 1970.12.13T12:45:50.123]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<timestamp>([1969.01.01T12:12:14.123, , 1970.12.13T12:45:50.123])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 76)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestampArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timestamp1 := time.Date(1969, 1, 1, 12, 12, 14, 123000000, time.UTC)
			timestamp2 := time.Date(1970, 12, 13, 12, 45, 50, 123000000, time.UTC)
			So(re.Get(0).Value(), ShouldEqual, timestamp1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, timestamp2)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(TIMESTAMP[], 0, 10).append!([take(timestamp(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<timestamp>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 76)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestampArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_nanotime(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(NANOTIME[], 0, 10).append!([[2012.01.01T12:12:14.001456793, 2012.01.01T13:24:59.003154697, 2012.01.01T12:45:59.100123456]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<nanotime>([12:12:14.001456793, 13:24:59.003154697, 12:45:59.100123456])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 77)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(NANOTIME[], 0, 10).append!([[1969.01.01T12:12:14.123, NULL, 1970.12.13T12:45:50.123]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<nanotime>([12:12:14.123000000, , 12:45:50.123000000])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 77)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timestamp1 := time.Date(1970, 1, 1, 12, 12, 14, 123000000, time.UTC)
			timestamp2 := time.Date(1970, 1, 1, 12, 45, 50, 123000000, time.UTC)
			So(re.Get(0).Value(), ShouldEqual, timestamp1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, timestamp2)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(NANOTIME[], 0, 10).append!([take(timestamp(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<nanotime>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 77)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimeArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_nanotimestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr=array(NANOTIMESTAMP[], 0, 10).append!([[2012.01.01T12:12:14.001, 2012.01.01T13:24:59.003, 2012.01.01T12:45:59.100]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<nanotimestamp>([2012.01.01T12:12:14.001000000, 2012.01.01T13:24:59.003000000, 2012.01.01T12:45:59.100000000])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 78)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestampArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(result.HashBucket(1, 1), ShouldEqual, 0)
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(NANOTIMESTAMP[], 0, 10).append!([[1969.01.01T12:12:14.123, NULL, 1970.12.13T12:45:50.123]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<nanotimestamp>([1969.01.01T12:12:14.123000000, , 1970.12.13T12:45:50.123000000])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 78)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestampArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			timestamp1 := time.Date(1969, 1, 1, 12, 12, 14, 123000000, time.UTC)
			timestamp2 := time.Date(1970, 12, 13, 12, 45, 50, 123000000, time.UTC)
			So(re.Get(0).Value(), ShouldEqual, timestamp1)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).Value(), ShouldEqual, timestamp2)
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(NANOTIMESTAMP[], 0, 10).append!([take(nanotimestamp(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<nanotimestamp>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 78)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestampArray")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_decimal32(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr = array(DECIMAL32(3)[], 0, 10).append!([[2.3, 4.5, 7.9]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal32>([2.300, 4.500, 7.900])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 101)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal32Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			formString := result.GetDataFormString()
			So(formString, ShouldEqual, "vector")
			rem := re.Data.Value()
			So(rem[0].(*model.Decimal32).Scale, ShouldEqual, 3)
			So(rem[1].(*model.Decimal32).Scale, ShouldEqual, 3)
			So(rem[2].(*model.Decimal32).Scale, ShouldEqual, 3)
			So(rem[0].(*model.Decimal32).Value, ShouldEqual, 2.3)
			So(rem[1].(*model.Decimal32).Value, ShouldEqual, 4.5)
			So(rem[2].(*model.Decimal32).Value, ShouldEqual, 7.9)
			So(re.Get(0).String(), ShouldEqual, "2.300")
			So(re.Get(1).String(), ShouldEqual, "4.500")
			So(re.Get(2).String(), ShouldEqual, "7.900")
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(DECIMAL32(3)[], 0, 10).append!([[2.3, NULL, 7.9]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal32>([2.300, , 7.900])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 101)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal32Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).String(), ShouldEqual, "2.300")
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).String(), ShouldEqual, "7.900")
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(DECIMAL32(3)[], 0, 10).append!([take(double(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal32>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 101)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal32Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_decimal64(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("arr = array(DECIMAL64(3)[], 0, 10).append!([[2.3, 4.5, 7.9]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal64>([2.300, 4.500, 7.900])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 102)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal64Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			formString := result.GetDataFormString()
			So(formString, ShouldEqual, "vector")
			rem := re.Data.Value()
			So(rem[0].(*model.Decimal64).Scale, ShouldEqual, 3)
			So(rem[1].(*model.Decimal64).Scale, ShouldEqual, 3)
			So(rem[2].(*model.Decimal64).Scale, ShouldEqual, 3)
			So(rem[0].(*model.Decimal64).Value, ShouldEqual, 2.3)
			So(rem[1].(*model.Decimal64).Value, ShouldEqual, 4.5)
			So(rem[2].(*model.Decimal64).Value, ShouldEqual, 7.9)
			So(re.Get(0).String(), ShouldEqual, "2.300")
			So(re.Get(1).String(), ShouldEqual, "4.500")
			So(re.Get(2).String(), ShouldEqual, "7.900")
		})
		Convey("Test_arrayVector_contain_null:", func() {
			s, err := db.RunScript("arr=array(DECIMAL64(3)[], 0, 10).append!([[2.3, NULL, 7.9]]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal64>([2.300, , 7.900])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 102)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal64Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).String(), ShouldEqual, "2.300")
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).String(), ShouldEqual, "7.900")
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(DECIMAL64(3)[], 0, 10).append!([take(double(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal64>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 102)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal64Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_arrayvector_Download_Datatype_decimal128(t *testing.T) {
	t.Parallel()
	Convey("Test_arrayvector_single_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_arrayVector:", func() {
			s, err := db.RunScript("re = bigarray(DECIMAL128(2)[], 0, 30).append!([[4, 92233720368547758, NULL, 100000000000000, NULL, -92233720368547758, -100000000000000, 3], [], [00i], [92233720368547758, 2, 100000000000000, -92233720368547758]]);re")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			So(re.Get(0).String(), ShouldEqual, "4.00")
			So(re.Get(1).String(), ShouldEqual, "92233720368547758.00")
			So(re.Get(2).String(), ShouldEqual, "")
			So(re.Get(3).String(), ShouldEqual, "100000000000000.00")
			So(re.Get(4).String(), ShouldEqual, "")
			So(re.Get(5).String(), ShouldEqual, "-92233720368547758.00")
			So(re.Get(6).String(), ShouldEqual, "-100000000000000.00")
			So(re.Get(7).String(), ShouldEqual, "3.00")
			re = result.GetVectorValue(1)
			So(re.Rows(), ShouldEqual, 1)
			So(re.Get(0).Value().(*model.Decimal128).String(), ShouldEqual, "")
			re = result.GetVectorValue(2)
			So(re.Rows(), ShouldEqual, 1)
			So(re.Get(0).Value().(*model.Decimal128).String(), ShouldEqual, "")
			re = result.GetVectorValue(3)
			So(re.Get(0).Value().(*model.Decimal128).String(), ShouldEqual, "92233720368547758.00")
			So(re.Get(1).Value().(*model.Decimal128).String(), ShouldEqual, "2.00")
			So(re.Get(2).Value().(*model.Decimal128).String(), ShouldEqual, "100000000000000.00")
			So(re.Get(3).Value().(*model.Decimal128).String(), ShouldEqual, "-92233720368547758.00")

			reType := result.GetDataType()
			So(reType, ShouldEqual, 103)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal128Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, model.DfVector)
			formString := result.GetDataFormString()
			So(formString, ShouldEqual, "vector")
		})
		Convey("Test_arrayVector_all_null:", func() {
			s, err := db.RunScript("arr=array(DECIMAL128(3)[], 0, 10).append!([take(double(), 3)]);arr")
			So(err, ShouldBeNil)
			result := s.(*model.Vector)
			re := result.GetVectorValue(0)
			assert.Equal(t, re.String(), "vector<decimal128>([, , ])")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 103)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "decimal128Array")
			form := result.GetDataForm()
			So(form, ShouldEqual, 1)
			So(re.Get(0).IsNull(), ShouldEqual, true)
			So(re.Get(1).IsNull(), ShouldEqual, true)
			So(re.Get(2).IsNull(), ShouldEqual, true)
		})
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_short(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{-1024, 1048, 1024, 0, -1024})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{0, -1024, model.NullShort, 10})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{model.NullShort, model.NullShort, model.NullShort, model.NullShort})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtShort+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST SHORT[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<shortArray>([[-1024, 1048, 1024, 0, -1024], [0, -1024, , 10], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_long(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{-1024, 1048576, 1048580, 1024, 1030, 65537, -1048579, 3000000})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{0, 1048576, model.NullLong, 3000000})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{model.NullLong, model.NullLong, model.NullLong, model.NullLong})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<longArray>([[-1024, 1048576, 1048580, 1024, 1030, 65537, -1048579, 3000000], [0, 1048576, , 3000000], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(typestr.String(), ShouldEqual, "string(FAST LONG[] VECTOR)")
		So(res.GetDataType(), ShouldEqual, model.DtLong+64)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_char(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{99, 10, 20, 0, 3})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{0, 19, model.NullChar, 10})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{model.NullChar, model.NullChar, model.NullChar, model.NullChar})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtChar+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST CHAR[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<charArray>([[99, 10, 20, 0, 3], [0, 19, , 10], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_bool(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{1, 0})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{0, 19, model.NullBool, 10})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{model.NullBool, model.NullBool, model.NullBool, model.NullBool})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtBool+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST BOOL[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<boolArray>([[true, false], [false, true, , true], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_float(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{2.365, 5.694})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{0.2354, 1.9, model.NullFloat, 1.0})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{model.NullFloat, model.NullFloat, model.NullFloat, model.NullFloat})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtFloat+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST FLOAT[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<floatArray>([[2.365, 5.694], [0.2354, 1.9, , 1], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_double(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{2.365, 5.694})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{0.2354, 1.9, model.NullDouble, 1.0})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{model.NullDouble, model.NullDouble, model.NullDouble, model.NullDouble})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDouble+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DOUBLE[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<doubleArray>([[2.365, 5.694], [0.2354, 1.9, , 1], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_date(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDate+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DATE[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<dateArray>([[1969.12.31, 1970.01.01, 1970.01.02, 2006.01.02, 2006.01.03, 2022.08.03], [1969.12.31, 1970.01.01, 1970.01.02, 2006.01.02, 2006.01.03, 2022.08.03, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_month(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtMonth+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST MONTH[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<monthArray>([[1969.12M, 1970.01M, 1970.01M, 2006.01M, 2006.01M, 2022.08M], [1969.12M, 1970.01M, 1970.01M, 2006.01M, 2006.01M, 2022.08M, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_datetime(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDatetime+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DATETIME[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<datetimeArray>([[1969.12.31T00:00:00, 1970.01.01T12:23:46, 1970.01.02T00:12:00, 2006.01.02T12:12:00, 2006.01.03T00:00:00, 2022.08.03T00:00:00], [1969.12.31T00:00:00, 1970.01.01T12:46:59, 1970.01.02T00:00:59, 2006.01.02T15:12:00, 2006.01.03T00:00:00, 2022.08.03T00:00:00, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_timestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtTimestamp+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST TIMESTAMP[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<timestampArray>([[1969.12.31T00:00:00.000, 1970.01.01T12:23:46.000, 1970.01.02T00:12:00.000, 2006.01.02T12:12:00.000, 2006.01.03T00:00:00.000, 2022.08.03T00:00:00.000], [1969.12.31T00:00:00.000, 1970.01.01T12:46:59.000, 1970.01.02T00:00:59.000, 2006.01.02T15:12:00.000, 2006.01.03T00:00:00.000, 2022.08.03T00:00:00.000, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_nanotimestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtNanoTimestamp+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST NANOTIMESTAMP[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<nanotimestampArray>([[1969.12.31T00:00:00.000000000, 1970.01.01T12:23:46.000102456, 1970.01.02T00:12:00.000000000, 2006.01.02T12:12:00.000000000, 2006.01.03T00:00:00.000000000, 2022.08.03T00:00:00.000000000], [1969.12.31T00:00:00.000000000, 1970.01.01T12:46:59.000000000, 1970.01.02T00:00:59.000000000, 2006.01.02T15:12:00.000000000, 2006.01.03T00:00:00.000000000, 2022.08.03T00:00:00.000000000, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_time(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtTime+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST TIME[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<timeArray>([[00:00:00.000, 12:23:46.000, 00:12:00.000, 12:12:00.000, 00:00:00.000, 00:00:00.000], [00:00:00.000, 12:46:59.000, 00:00:59.000, 15:12:00.000, 00:00:00.000, 00:00:00.000, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_second(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtSecond+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST SECOND[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<secondArray>([[00:00:00, 12:23:46, 00:12:00, 12:12:00, 00:00:00, 00:00:00], [00:00:00, 12:46:59, 00:00:59, 15:12:00, 00:00:00, 00:00:00, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_minute(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtMinute+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST MINUTE[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<minuteArray>([[00:00m, 12:23m, 00:12m, 12:12m, 00:00m, 00:00m], [00:00m, 12:46m, 00:00m, 15:12m, 00:00m, 00:00m, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_nanotime(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtNanoTime+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST NANOTIME[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<nanotimeArray>([[00:00:00.000000000, 12:23:46.000102456, 00:12:00.000000000, 12:12:00.000000000, 00:00:00.000000000, 00:00:00.000000000], [00:00:00.000000000, 12:46:59.000000000, 00:00:59.000000000, 15:12:00.000000000, 00:00:00.000000000, 00:00:00.000000000, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_datehour(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 23, 46, 102456, time.UTC), time.Date(1970, 1, 2, 0, 12, 0, 0, time.UTC), time.Date(2006, 1, 2, 12, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 46, 59, 0, time.UTC), time.Date(1970, 1, 2, 0, 0, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 12, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC), time.Date(2022, 8, 3, 0, 0, 0, 0, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDateHour+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DATEHOUR[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<datehourArray>([[1969.12.31T00, 1970.01.01T12, 1970.01.02T00, 2006.01.02T12, 2006.01.03T00, 2022.08.03T00], [1969.12.31T00, 1970.01.01T12, 1970.01.02T00, 2006.01.02T15, 2006.01.03T00, 2022.08.03T00, ], [, , , ]])")
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldEqual, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldEqual, vec2.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_decimal32(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 6, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 5, Value: []float64{0.2354, 1.9, model.NullDecimal32Value, 1.0}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 3, Value: []float64{model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDecimal32+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL32[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		ex := "vector<decimal32Array>([[3.212350, -1.000000, ], [0.235400, 1.900000, , 1.000000], [, , , ]])"
		So(m, ShouldEqual, ex)

		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldResemble, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		vecm, _ := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 6, Value: []float64{0.2354, 1.9, model.NullDecimal32Value, 1.0}})
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldResemble, vecm.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Vector_UpLoad_array_vector_decimal64(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 6, Value: []float64{3.21235, -1, model.NullDecimal64Value}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 8, Value: []float64{0.2354, 1.9, model.NullDecimal64Value, 1.0}})
		So(err, ShouldBeNil)
		Println(vec2.Value())
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 3, Value: []float64{model.NullDecimal64Value, model.NullDecimal64Value, model.NullDecimal64Value, model.NullDecimal64Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDecimal64+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL64[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		ex := "vector<decimal64Array>([[3.212350, -1.000000, ], [0.235400, 1.900000, , 1.000000], [, , , ]])"
		So(m, ShouldEqual, ex)
		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldResemble, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		vecm, _ := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 6, Value: []float64{0.2354, 1.9, model.NullDecimal64Value, 1.0}})
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldResemble, vecm.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_decimal32_scale_min_in_first_vector(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 3, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 4, Value: []float64{0.2354, 1.9, model.NullDecimal32Value, 1.0}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 8, Value: []float64{model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDecimal32+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL32[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		ex := "vector<decimal32Array>([[3.212, -1.000, ], [0.235, 1.900, , 1.000], [, , , ]])"
		So(m, ShouldEqual, ex)

		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldResemble, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		vecm, _ := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 3, Value: []float64{0.235, 1.900, model.NullDecimal32Value, 1.000}})
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldResemble, vecm.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_decimal32_scale_zero_in_first_vector(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 0, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 4, Value: []float64{2.354, 1.9, model.NullDecimal32Value, 1.0}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 8, Value: []float64{model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDecimal32+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL32[] VECTOR)")
		re := res.(*model.Vector)
		m := re.String()
		ex := "vector<decimal32Array>([[3, -1, ], [2, 1, , 1], [, , , ]])"
		So(m, ShouldEqual, ex)

		for i := 0; i < vec1.Len(); i++ {
			So(re.Get(i).Value(), ShouldResemble, vec1.Value()[i])
		}
		result1 := re.GetVectorValue(1)
		vecm, _ := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 0, Value: []float64{2, 1, model.NullDecimal32Value, 1}})
		for i := 0; i < vec2.Len(); i++ {
			So(result1.Get(i).Value(), ShouldResemble, vecm.Value()[i])
		}
		result2 := re.GetVectorValue(2)
		So(result2.Get(0).IsNull(), ShouldBeTrue)
		So(result2.Get(1).IsNull(), ShouldBeTrue)
		So(result2.Get(2).IsNull(), ShouldBeTrue)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal32_scale_negative(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: -2, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Scale out of bound(valid range: [0, 9], but get: -2)")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal64_scale_negative(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: -2, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Scale out of bound(valid range: [0, 18], but get: -2)")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal64_scale_12(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		m, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 12, Value: []float64{12345.12345, -1, model.NullDecimal64Value}})
		So(err, ShouldBeNil)
		result := model.NewArrayVector([]*model.Vector{model.NewVector(m)})
		s := model.NewVectorWithArrayVector(result)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(err, ShouldBeNil)
		So(res.GetDataType(), ShouldEqual, model.DtDecimal64+64)
		re := res.(*model.Vector)
		So(re.String(), ShouldEqual, "vector<decimal64Array>([[12345.123450000000, -1.000000000000, ]])")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal32_scale_10(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 10, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Scale out of bound(valid range: [0, 9], but get: 10)")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal64_scale_19(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 19, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Scale out of bound(valid range: [0, 18], but get: 19)")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_decimal32_scale_9_in_first_vector(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 9, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Decimal math overflow")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_decimal64_scale_17_in_first_vector(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 17, Value: []float64{3.21235, -1, model.NullDecimal32Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Decimal math overflow")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_array_vector_decimal128(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 30, Value: []string{"-1.123123123123123123123123123123123123123", "0", "", model.NullDecimal128Value}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 20, Value: []string{"-1.123123123123123123123123123123123123123", "0", "", model.NullDecimal128Value}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 0, Value: []string{model.NullDecimal128Value, model.NullDecimal128Value, model.NullDecimal128Value, model.NullDecimal128Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)}) // 默认按第一个vector中的decimal数据scale作为整个arrayVector的scale
		s := model.NewVectorWithArrayVector(NewData)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		_, err = db.RunScript(`ex = array(DECIMAL128(30)[],0,3).append!([decimal128(["-1.123123123123123123123123123123123123123", "0", "", NULL],30), decimal128(["-1.123123123123123123123123123123123123123", "0", "", NULL],20), take(decimal128(NULL, 0), 4)]);print(ex);print(s);` +
			`assert 1, eqObj(ex, s)`)
		So(err, ShouldBeNil)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal128_scale_negative(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: -2, Value: []string{model.NullDecimal128Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Scale out of bound(valid range: [0, 38], but get: -2)")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_UpLoad_vector_decimal128_scale_gt_38(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 40, Value: []string{model.NullDecimal128Value}})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "Scale out of bound(valid range: [0, 38], but get: 40)")
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_int(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("123 -21 0 1024 12")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("123 NULL 0 NULL 12")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("take(00i, 10)")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []int32{123, -21, 0, 1024, 12}
		ex2 := []int32{123, model.NullInt, 0, model.NullInt, 12}
		for i := 0; i < 5; i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < 5; i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < 5; i++ {
			So(res3[i], ShouldEqual, -2147483648)
		}
		vec4, _ := db.RunScript("1..1020")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == i+1 {
				k++
			}
		}
		vec5, _ := db.RunScript("1..1030")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1020; i++ {
			if re5[i] == i+1 {
				k2++
			}
		}
		vec6, _ := db.RunScript("1..1048580")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1020; i++ {
			if re6[i] == i+1 {
				k3++
			}
		}
		vec7, _ := db.RunScript("1..65538")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 1020; i++ {
			if re7[i] == i+1 {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_long(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("long(123 -21 0 1024 12)")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("long(123 NULL 0 NULL 12)")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("long(take(00i, 10))")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "long")
		So(result2.GetDataTypeString(), ShouldEqual, "long")
		So(result3.GetDataTypeString(), ShouldEqual, "long")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []int64{123, -21, 0, 1024, 12}
		ex2 := []int64{123, model.NullLong, 0, model.NullLong, 12}
		for i := 0; i < 5; i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < 5; i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < 5; i++ {
			So(res3[i], ShouldEqual, -9223372036854775808)
		}
		vec4, _ := db.RunScript("1..1020")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == i+1 {
				k++
			}
		}
		vec5, _ := db.RunScript("1..1030")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1020; i++ {
			if re5[i] == i+1 {
				k2++
			}
		}
		vec6, _ := db.RunScript("1..1048580")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1020; i++ {
			if re6[i] == i+1 {
				k3++
			}
		}
		vec7, _ := db.RunScript("1..65538")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 1020; i++ {
			if re7[i] == i+1 {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_short(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("short(123 -21 0 1024 12)")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("short(123 NULL 0 NULL 12)")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("short(take(00i, 10))")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "short")
		So(result2.GetDataTypeString(), ShouldEqual, "short")
		So(result3.GetDataTypeString(), ShouldEqual, "short")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []int16{123, -21, 0, 1024, 12}
		ex2 := []int16{123, model.NullShort, 0, model.NullShort, 12}
		for i := 0; i < 5; i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < 5; i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < 5; i++ {
			So(res3[i], ShouldEqual, -32768)
		}
		vec4, _ := db.RunScript("1..1020")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == i+1 {
				k++
			}
		}
		vec5, _ := db.RunScript("1..1030")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1020; i++ {
			if re5[i] == i+1 {
				k2++
			}
		}
		vec6, _ := db.RunScript("1..1048580")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1020; i++ {
			if re6[i] == i+1 {
				k3++
			}
		}
		vec7, _ := db.RunScript("1..65538")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 1020; i++ {
			if re7[i] == i+1 {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_char(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("char(12 1 0 24 2)")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("char(3 NULL 0 NULL 2)")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("char(take(00i, 10))")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "char")
		So(result2.GetDataTypeString(), ShouldEqual, "char")
		So(result3.GetDataTypeString(), ShouldEqual, "char")
		res1 := result1.GetRawValue()
		// res2 := result2.GetRawValue()
		// res3 := result3.GetRawValue()
		ex1 := []byte{12, 1, 0, 24, 2}
		// ex2 := []byte{3, model.NullChar, 0, model.NullChar, 2}
		for i := 0; i < 5; i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		// for i := 0; i < 5; i++ {
		// 	So(res2[i], ShouldEqual, ex2[i])
		// }
		// for i := 0; i < 5; i++ {
		// 	So(res3[i], ShouldEqual, -128)
		// }
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_bool(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("true false")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("true false true NULL")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("take(bool(), 10)")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "bool")
		So(result2.GetDataTypeString(), ShouldEqual, "bool")
		So(result3.GetDataTypeString(), ShouldEqual, "bool")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		// res3 := result3.GetRawValue()
		So(res1[0], ShouldBeTrue)
		So(res1[1], ShouldBeFalse)
		So(res2[0], ShouldBeTrue)
		So(res2[1], ShouldBeFalse)
		So(res2[2], ShouldBeTrue)
		// So(res2[3], ShouldEqual, model.NullBool)
		// for i := 0; i < 5; i++ {
		// 	So(res3[i], ShouldEqual, -128)
		// }
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_double(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("double(1.2345823 -2.125451 0.154646 1.2365024 1.23562)")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("double(1.2345823 NULL 0.2356564 NULL 1.235646462)")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("take(double(), 10)")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "double")
		So(result2.GetDataTypeString(), ShouldEqual, "double")
		So(result3.GetDataTypeString(), ShouldEqual, "double")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []float64{1.2345823, -2.125451, 0.154646, 1.2365024, 1.23562}
		ex2 := []float64{1.2345823, model.NullDouble, 0.2356564, model.NullDouble, 1.235646462}
		for i := 0; i < 5; i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < 5; i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < 5; i++ {
			So(res3[i], ShouldEqual, model.NullDouble)
		}
		vec4, _ := db.RunScript("take(1.23645897, 1020)")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == 1.23645897 {
				k++
			}
		}
		vec5, _ := db.RunScript("take(1.23645897, 1030)")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1030; i++ {
			if re5[i] == 1.23645897 {
				k2++
			}
		}
		vec6, _ := db.RunScript("take(1.23645897, 1048580)")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1048580; i++ {
			if re6[i] == 1.23645897 {
				k3++
			}
		}
		vec7, _ := db.RunScript("take(1.23645897, 65538)")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 65538; i++ {
			if re7[i] == i+1 {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_float(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("float(1.2345823 -2.125451 0.154646 1.2365024 1.23562)")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("float(1.2345823 NULL 0.2356564 NULL 1.235646462)")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("take(float(), 10)")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "float")
		So(result2.GetDataTypeString(), ShouldEqual, "float")
		So(result3.GetDataTypeString(), ShouldEqual, "float")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []float32{1.2345823, -2.125451, 0.154646, 1.2365024, 1.23562}
		ex2 := []float32{1.2345823, model.NullFloat, 0.2356564, model.NullFloat, 1.235646462}
		for i := 0; i < 5; i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < 5; i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < 5; i++ {
			So(res3[i], ShouldEqual, model.NullFloat)
		}
		vec4, _ := db.RunScript("take(1.23645897, 1020)")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == 1.23645897 {
				k++
			}
		}
		vec5, _ := db.RunScript("take(1.23645897, 1030)")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1030; i++ {
			if re5[i] == 1.23645897 {
				k2++
			}
		}
		vec6, _ := db.RunScript("take(1.23645897, 1048580)")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1048580; i++ {
			if re6[i] == 1.23645897 {
				k3++
			}
		}
		vec7, _ := db.RunScript("take(1.23645897, 65538)")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 65538; i++ {
			if re7[i] == i+1 {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_Date(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("1970.01.01 1969.12.31 1972.12.31")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("1970.01.01 NULL 1969.12.31 NULL 1972.12.31")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("take(date(), 10)")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "date")
		So(result2.GetDataTypeString(), ShouldEqual, "date")
		So(result3.GetDataTypeString(), ShouldEqual, "date")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []time.Time{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1972, 12, 31, 0, 0, 0, 0, time.UTC)}
		ex2 := []time.Time{time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime, time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), model.NullTime, time.Date(1972, 12, 31, 0, 0, 0, 0, time.UTC)}
		for i := 0; i < len(ex1); i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < len(ex2); i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < len(res3); i++ {
			So(res3[i], ShouldEqual, model.NullTime)
		}
		vec4, _ := db.RunScript("take(1970.01.01, 1020)")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k++
			}
		}
		vec5, _ := db.RunScript("take(1970.01.01, 1030)")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1030; i++ {
			if re5[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k2++
			}
		}
		vec6, _ := db.RunScript("take(1970.01.01, 1048580)")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1048580; i++ {
			if re6[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k3++
			}
		}
		vec7, _ := db.RunScript("take(1970.01.01, 65538)")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 65538; i++ {
			if re7[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_Vector_Download_Datatype_DateTime(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := db.RunScript("datetime(1970.01.01T12:12:12 1969.12.31T12:59:12 1972.12.31T23:12:45)")
		So(err, ShouldBeNil)
		vec2, err := db.RunScript("datetime([1970.01.01T12:23:12, NULL, 1969.12.31T12:59:12, NULL, 1972.12.31T23:12:45])")
		So(err, ShouldBeNil)
		vec3, err := db.RunScript("take(datetime(), 10)")
		So(err, ShouldBeNil)
		result1 := vec1.(*model.Vector)
		result2 := vec2.(*model.Vector)
		result3 := vec3.(*model.Vector)
		So(result1.GetDataTypeString(), ShouldEqual, "datetime")
		So(result2.GetDataTypeString(), ShouldEqual, "datetime")
		So(result3.GetDataTypeString(), ShouldEqual, "datetime")
		res1 := result1.GetRawValue()
		res2 := result2.GetRawValue()
		res3 := result3.GetRawValue()
		ex1 := []time.Time{time.Date(1970, 1, 1, 12, 12, 12, 0, time.UTC), time.Date(1969, 12, 31, 12, 59, 12, 0, time.UTC), time.Date(1972, 12, 31, 23, 12, 45, 0, time.UTC)}
		ex2 := []time.Time{time.Date(1970, 1, 1, 12, 23, 12, 0, time.UTC), model.NullTime, time.Date(1969, 12, 31, 12, 59, 12, 0, time.UTC), model.NullTime, time.Date(1972, 12, 31, 23, 12, 45, 0, time.UTC)}
		for i := 0; i < len(ex1); i++ {
			So(res1[i], ShouldEqual, ex1[i])
		}
		for i := 0; i < len(ex2); i++ {
			So(res2[i], ShouldEqual, ex2[i])
		}
		for i := 0; i < len(res3); i++ {
			So(res3[i], ShouldEqual, model.NullTime)
		}
		vec4, _ := db.RunScript("take(1970.01.01, 1020)")
		result4 := vec4.(*model.Vector)
		re4 := result4.GetRawValue()
		var k int
		for i := 0; i < 1020; i++ {
			if re4[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k++
			}
		}
		vec5, _ := db.RunScript("take(1970.01.01, 1030)")
		result5 := vec5.(*model.Vector)
		re5 := result5.GetRawValue()
		var k2 int
		for i := 0; i < 1030; i++ {
			if re5[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k2++
			}
		}
		vec6, _ := db.RunScript("take(1970.01.01, 1048580)")
		result6 := vec6.(*model.Vector)
		re6 := result6.GetRawValue()
		var k3 int
		for i := 0; i < 1048580; i++ {
			if re6[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k3++
			}
		}
		vec7, _ := db.RunScript("take(1970.01.01, 65538)")
		result7 := vec7.(*model.Vector)
		re7 := result7.GetRawValue()
		var k4 int
		for i := 0; i < 65538; i++ {
			if re7[i] == time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC) {
				k4++
			}
		}
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_double(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{2.365878945, model.NullDouble, -5.69154974})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{-0.2354, 1.925498941, 0, model.NullDouble, 1.0})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{model.NullDouble, model.NullDouble, model.NullDouble, model.NullDouble})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, 2.365878945)
		So(re.GetRawValue()[1], ShouldEqual, model.NullDouble)
		So(re.GetRawValue()[2], ShouldEqual, -5.69154974)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDouble+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DOUBLE[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<doubleArray>([[2.365878945, , -5.69154974], [-0.2354, 1.925498941, 0, , 1], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullDouble)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullDouble)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullDouble)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_float(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{2.365878945, model.NullFloat, -5.69154974})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{-0.2354, 1.925498941, 0, model.NullFloat, 1.0})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtFloat, []float32{model.NullFloat, model.NullFloat, model.NullFloat, model.NullFloat})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldAlmostEqual, 2.365878945, 0.000001)
		So(re.GetRawValue()[1], ShouldEqual, model.NullFloat)
		So(re.GetRawValue()[2], ShouldAlmostEqual, -5.69154974, 0.000001)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtFloat+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST FLOAT[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<floatArray>([[2.365879, , -5.69155], [-0.2354, 1.925499, 0, , 1], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullFloat)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullFloat)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullFloat)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_int(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{2, model.NullInt, -5})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{0, 5, 0, model.NullInt, 1})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{model.NullInt, model.NullInt, model.NullInt, model.NullInt})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, 2)
		So(re.GetRawValue()[1], ShouldEqual, model.NullInt)
		So(re.GetRawValue()[2], ShouldEqual, -5)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtInt+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST INT[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<intArray>([[2, , -5], [0, 5, 0, , 1], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullInt)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullInt)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullInt)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_short(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{2, model.NullShort, -5})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{0, 5, 0, model.NullShort, 1})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtShort, []int16{model.NullShort, model.NullShort, model.NullShort, model.NullShort})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, 2)
		So(re.GetRawValue()[1], ShouldEqual, model.NullShort)
		So(re.GetRawValue()[2], ShouldEqual, -5)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtShort+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST SHORT[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<shortArray>([[2, , -5], [0, 5, 0, , 1], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullShort)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullShort)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullShort)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_long(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{2, model.NullLong, -5})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{0, 5, 0, model.NullLong, 1})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtLong, []int64{model.NullLong, model.NullLong, model.NullLong, model.NullLong})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, 2)
		So(re.GetRawValue()[1], ShouldEqual, model.NullLong)
		So(re.GetRawValue()[2], ShouldEqual, -5)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtLong+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST LONG[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<longArray>([[2, , -5], [0, 5, 0, , 1], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullLong)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullLong)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullLong)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_char(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{2, model.NullChar, 5})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{0, 5, 0, model.NullChar, 1})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtChar, []byte{model.NullChar, model.NullChar, model.NullChar, model.NullChar})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, 2)
		// So(re.GetRawValue()[1], ShouldEqual, model.NullChar)
		So(re.GetRawValue()[2], ShouldEqual, 5)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtChar+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST CHAR[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<charArray>([[2, , 5], [0, 5, 0, , 1], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		// result3 := re.GetVectorValue(2)
		// So(result3.GetRawValue()[0], ShouldEqual, model.NullChar)
		// So(result3.GetRawValue()[1], ShouldEqual, model.NullChar)
		// So(result3.GetRawValue()[2], ShouldEqual, model.NullChar)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_bool(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtBool, []bool{true, true, false})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtBool, []bool{true, false, true})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtBool, []byte{model.NullBool, model.NullBool, model.NullBool, model.NullBool})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, true)
		So(re.GetRawValue()[1], ShouldEqual, true)
		So(re.GetRawValue()[2], ShouldEqual, false)
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtBool+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST BOOL[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<boolArray>([[true, true, false], [true, false, true], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		// result3 := re.GetVectorValue(2)
		// So(result3.GetRawValue()[0], ShouldEqual, model.nullBool)
		// So(result3.GetRawValue()[1], ShouldEqual, model.nullBool)
		// So(result3.GetRawValue()[2], ShouldEqual, model.nullBool)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_date(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDate+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DATE[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<dateArray>([[1970.01.01, 1969.01.01, 2012.01.01], [, 1969.01.01, 2012.01.01], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_datetime(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDatetime+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DATETIME[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<datetimeArray>([[1970.01.01T12:23:45, 1969.01.01T12:23:45, 2012.01.01T12:23:45], [, 1969.01.01T12:23:45, 2012.01.01T12:23:45], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_datehour(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 0, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 0, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1969, 1, 1, 12, 0, 0, 0, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(2012, 1, 1, 12, 0, 0, 0, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDateHour+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DATEHOUR[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<datehourArray>([[1970.01.01T12, 1969.01.01T12, 2012.01.01T12], [, 1969.01.01T12, 2012.01.01T12], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_timestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 956125463, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 125123456, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1969, 1, 1, 12, 23, 45, 999000000, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(2012, 1, 1, 12, 23, 45, 959000000, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtTimestamp+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST TIMESTAMP[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<timestampArray>([[1970.01.01T12:23:45.999, 1969.01.01T12:23:45.999, 2012.01.01T12:23:45.959], [, 1969.01.01T12:23:45.956, 2012.01.01T12:23:45.125], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_nanotimestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 956125463, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 125123456, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtNanoTimestamp+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST NANOTIMESTAMP[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<nanotimestampArray>([[1970.01.01T12:23:45.999000000, 1969.01.01T12:23:45.999123456, 2012.01.01T12:23:45.959836563], [, 1969.01.01T12:23:45.956125463, 2012.01.01T12:23:45.125123456], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_nanotime(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 956125463, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 125123456, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 999123456, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 959836563, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtNanoTime+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST NANOTIME[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<nanotimeArray>([[12:23:45.999000000, 12:23:45.999123456, 12:23:45.959836563], [, 12:23:45.956125463, 12:23:45.125123456], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_time(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 956125463, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 125123456, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 959000000, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtTime+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST TIME[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<timeArray>([[12:23:45.999, 12:23:45.999, 12:23:45.959], [, 12:23:45.956, 12:23:45.125], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_second(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 956125463, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 125123456, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 45, 0, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtSecond+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST SECOND[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<secondArray>([[12:23:45, 12:23:45, 12:23:45], [, 12:23:45, 12:23:45], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_minute(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{time.Date(1970, 1, 1, 12, 23, 45, 999000000, time.UTC), time.Date(1969, 1, 1, 12, 23, 45, 999123456, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 959836563, time.UTC)})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{model.NullTime, time.Date(1969, 1, 1, 12, 23, 45, 956125463, time.UTC), time.Date(2012, 1, 1, 12, 23, 45, 125123456, time.UTC)})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{model.NullTime, model.NullTime, model.NullTime, model.NullTime})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		So(re.GetRawValue()[0], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 0, 0, time.UTC))
		So(re.GetRawValue()[1], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 0, 0, time.UTC))
		So(re.GetRawValue()[2], ShouldEqual, time.Date(1970, 1, 1, 12, 23, 0, 0, time.UTC))
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtMinute+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST MINUTE[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<minuteArray>([[12:23m, 12:23m, 12:23m], [, 12:23m, 12:23m], [, , , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldEqual, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldEqual, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[1], ShouldEqual, model.NullTime)
		So(result3.GetRawValue()[2], ShouldEqual, model.NullTime)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_decimal32(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 3, Value: []float64{2.365878945, model.NullDecimal32Value, -5.69154974}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 3, Value: []float64{2.365878945, model.NullDecimal32Value, -5.69154974}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal32, &model.Decimal32s{Scale: 3, Value: []float64{model.NullDecimal32Value, model.NullDecimal32Value, model.NullDecimal32Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		m1, _ := model.NewDataType(model.DtDecimal32, &model.Decimal32{Scale: 3, Value: 2.365})
		m2, _ := model.NewDataType(model.DtDecimal32, &model.Decimal32{Scale: 3, Value: -5.691})
		So(re.GetRawValue()[0], ShouldResemble, model.NewScalar(m1).Value())
		So(re.GetRawValue()[1].(*model.Decimal32).Value, ShouldEqual, model.NullDecimal32Value)
		So(re.GetRawValue()[2], ShouldResemble, model.NewScalar(m2).Value())
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDecimal32+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL32[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<decimal32Array>([[2.365, , -5.691], [2.365, , -5.691], [, , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldResemble, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldResemble, vec2.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0].(*model.Decimal32).Value, ShouldEqual, model.NullDecimal32Value)
		So(result3.GetRawValue()[1].(*model.Decimal32).Value, ShouldEqual, model.NullDecimal32Value)
		So(result3.GetRawValue()[2].(*model.Decimal32).Value, ShouldEqual, model.NullDecimal32Value)
		So(db.Close(), ShouldBeNil)
	})
}
func Test_GetRawValue_UpLoad_array_vector_decimal64(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 3, Value: []float64{2.365878945, model.NullDecimal64Value, -5.69154974}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 6, Value: []float64{2.365878945, model.NullDecimal64Value, -5.69154974}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 2, Value: []float64{model.NullDecimal64Value, model.NullDecimal64Value, model.NullDecimal64Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		m1, _ := model.NewDataType(model.DtDecimal64, &model.Decimal64{Scale: 3, Value: 2.365})
		m2, _ := model.NewDataType(model.DtDecimal64, &model.Decimal64{Scale: 3, Value: -5.691})
		So(re.GetRawValue()[0], ShouldResemble, model.NewScalar(m1).Value())
		So(re.GetRawValue()[1].(*model.Decimal64).Value, ShouldEqual, model.NullDecimal64Value)
		So(re.GetRawValue()[2], ShouldResemble, model.NewScalar(m2).Value())
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, model.DtDecimal64+64)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL64[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<decimal64Array>([[2.365, , -5.691], [2.365, , -5.691], [, , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldResemble, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		vec2M, _ := model.NewDataTypeListFromRawData(model.DtDecimal64, &model.Decimal64s{Scale: 3, Value: []float64{2.365878945, model.NullDecimal64Value, -5.69154974}})

		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldResemble, vec2M.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0].(*model.Decimal64).Value, ShouldEqual, model.NullDecimal64Value)
		So(result3.GetRawValue()[1].(*model.Decimal64).Value, ShouldEqual, model.NullDecimal64Value)
		So(result3.GetRawValue()[2].(*model.Decimal64).Value, ShouldEqual, model.NullDecimal64Value)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_GetRawValue_UpLoad_array_vector_decimal128(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_array_vector:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		vec1, err := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 3, Value: []string{"2.365878945", model.NullDecimal128Value, "-5.69154974"}})
		So(err, ShouldBeNil)
		vec2, err := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 6, Value: []string{"2.365878945", model.NullDecimal128Value, "-5.69154974"}})
		So(err, ShouldBeNil)
		vec3, err := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 2, Value: []string{model.NullDecimal128Value, model.NullDecimal128Value, model.NullDecimal128Value}})
		So(err, ShouldBeNil)
		NewData := model.NewArrayVector([]*model.Vector{model.NewVector(vec1), model.NewVector(vec2), model.NewVector(vec3)})
		s := model.NewVectorWithArrayVector(NewData)
		re := model.NewVector(vec1)
		m1, _ := model.NewDataType(model.DtDecimal128, &model.Decimal128{Scale: 3, Value: "2.365"})
		m2, _ := model.NewDataType(model.DtDecimal128, &model.Decimal128{Scale: 3, Value: "-5.691"})
		So(re.GetRawValue()[0], ShouldResemble, model.NewScalar(m1).Value())
		So(re.GetRawValue()[1].(*model.Decimal128).Value, ShouldEqual, model.NullDecimal128Value)
		So(re.GetRawValue()[2], ShouldResemble, model.NewScalar(m2).Value())
		_, err = db.Upload(map[string]model.DataForm{"s": s})
		So(err, ShouldBeNil)
		res, err := db.RunScript("s")
		So(res.GetDataType(), ShouldEqual, 103)
		So(err, ShouldBeNil)
		typestr, _ := db.RunScript("typestr(s)")
		So(typestr.String(), ShouldEqual, "string(FAST DECIMAL128[] VECTOR)")
		re = res.(*model.Vector)
		m := re.String()
		So(m, ShouldEqual, "vector<decimal128Array>([[2.365, , -5.691], [2.365, , -5.691], [, , ]])")
		result1 := re.GetVectorValue(0)
		for i := 0; i < vec1.Len(); i++ {
			So(result1.GetRawValue()[i], ShouldResemble, vec1.Value()[i])
		}
		result2 := re.GetVectorValue(1)
		vec2M, _ := model.NewDataTypeListFromRawData(model.DtDecimal128, &model.Decimal128s{Scale: 3, Value: []string{"2.365878945", model.NullDecimal128Value, "-5.69154974"}})

		for i := 0; i < vec2.Len(); i++ {
			So(result2.GetRawValue()[i], ShouldResemble, vec2M.Value()[i])
		}
		result3 := re.GetVectorValue(2)
		So(result3.GetRawValue()[0].(*model.Decimal128).Value, ShouldEqual, model.NullDecimal128Value)
		So(result3.GetRawValue()[1].(*model.Decimal128).Value, ShouldEqual, model.NullDecimal128Value)
		So(result3.GetRawValue()[2].(*model.Decimal128).Value, ShouldEqual, model.NullDecimal128Value)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_huge_val(t *testing.T) {
	t.Parallel()
	Convey("Test_vector_big_string_symbol_blob:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_vector_big_string:", func() {
			res, _ := db.RunScript("a = array(STRING,10).append!(string(concat(take(\"1\", 256 * 1024))));a")

			result := res.(*model.Vector)
			So(result.RowCount, ShouldEqual, 11)
			val11 := result.Get(10)
			So(val11.String(), ShouldEqual, strings.Repeat("1", 256*1024))
			for i := 0; i < 11; i++ {
				if i != 10 {
					kval := result.Get(i)
					So(kval.Value(), ShouldEqual, "")
				}
			}
			_, err := db.Upload(map[string]model.DataForm{"b": result})
			So(err.Error(), ShouldContainSubstring, "Serialized string length must less than 256k bytes.")
		})
		Convey("Test_vector_big_blob:", func() {
			res, _ := db.RunScript("a = array(BLOB,10).append!(blob(concat(take(\"123&#@!^%;d《》中文\",100000))));a")

			result := res.(*model.Vector)
			So(result.RowCount, ShouldEqual, 11)
			val11 := result.Get(10)
			So(val11.String(), ShouldEqual, strings.Repeat("123&#@!^%;d《》中文", 100000))
			for i := 0; i < 11; i++ {
				if i != 10 {
					kval := result.Get(i)
					So(kval.Value(), ShouldEqual, []uint8(nil))
				}
			}
			db.Upload(map[string]model.DataForm{"b": result})
			ans, _ := db.RunScript("eqObj(a,b)")
			So(ans.(*model.Scalar).Value(), ShouldBeTrue)
		})
		Convey("Test_vector_big_symbol:", func() {
			res, err := db.RunScript("a = array(SYMBOL,10).append!(symbol([concat(take(\"123&#@!^%;d《》中文\",100000))])[0]);a")
			So(err, ShouldBeNil)
			result := res.(*model.Vector)
			So(result.RowCount, ShouldEqual, 11)
			val11 := result.Get(10)
			So(val11.String(), ShouldEqual, strings.Repeat("123&#@!^%;d《》中文", 100000))
			for i := 0; i < 11; i++ {
				if i != 10 {
					kval := result.Get(i)
					So(kval.Value(), ShouldEqual, "")
				}
			}
			var str string
			for i := 0; i < 300000; i++ {
				str += "a"
			}
			rawdata, _ := model.NewDataTypeListFromRawData(model.DtSymbol, []string{str, str + "123"})
			s := model.NewVector(rawdata)
			_, err = db.Upload(map[string]model.DataForm{"s": s})
			So(err.Error(), ShouldContainSubstring, "Serialized string length must less than 256k bytes.")
		})

		So(db.Close(), ShouldBeNil)
	})
}

func Test_Vector_gt65535(t *testing.T) {
	t.Parallel()
	Convey("Test_Vector_gt65535:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		tests := []struct {
			typestr string
			ex_type model.DataTypeByte
			s       string
			ex_val  string
		}{
			{"bool", model.DtBool, "default", "true"},
			{"char", model.DtChar, "default", "1"},
			{"short", model.DtShort, "default", "1"},
			{"int", model.DtInt, "default", "1"},
			{"long", model.DtLong, "default", "1"},
			{"float", model.DtFloat, "default", "1"},
			{"double", model.DtDouble, "default", "1"},
			{"date", model.DtDate, "default", "1970.01.02"},
			{"month", model.DtMonth, "default", "0000.02M"},
			{"time", model.DtTime, "default", "00:00:00.001"},
			{"minute", model.DtMinute, "default", "00:01m"},
			{"second", model.DtSecond, "default", "00:00:01"},
			{"datetime", model.DtDatetime, "default", "1970.01.01T00:00:01"},
			{"timestamp", model.DtTimestamp, "default", "1970.01.01T00:00:00.001"},
			{"nanotime", model.DtNanoTime, "default", "00:00:00.000000001"},
			{"nanotimestamp", model.DtNanoTimestamp, "default", "1970.01.01T00:00:00.000000001"},
			{"uuid", model.DtUUID, "", "5d212a78-cc48-e3b1-4235-b4d91473ee87"},
			{"ipaddr", model.DtIP, "", "1.1.1.1"},
			{"int128", model.DtInt128, "", "e1671797c52e15f763380b45e841ec32"},
			{"symbol", model.DtSymbol + 128, "", "abc"}, // symbol_extend
			{"string", model.DtString, "", "abc"},
			{"blob", model.DtBlob, "", "abc"},
			{"decimal32(6)", model.DtDecimal32, "default", "1.000000"},
			{"decimal64(16)", model.DtDecimal64, "default", "1.0000000000000000"},
			{"decimal128(26)", model.DtDecimal128, "default", "1.00000000000000000000000000"},
		}
		for _, test := range tests {
			test := test
			Convey(fmt.Sprintf("test download vector with type %s", test.typestr), func() {
				if test.s == "default" {
					test.s = fmt.Sprintf("array(%s, 70000,, 1)", strings.ToUpper(test.typestr))
				} else {
					if test.typestr == "string" || test.typestr == "symbol" {
						test.s = fmt.Sprintf("array(%s, 70000,, 'abc')", strings.ToUpper(test.typestr))
					} else if test.typestr == "blob" {
						test.s = "take(blob(['abc']), 70000)"
					} else if test.typestr == "uuid" {
						test.s = "take(uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87']), 70000)"
					} else if test.typestr == "ipaddr" {
						test.s = "take(ipaddr(['1.1.1.1']), 70000)"
					} else if test.typestr == "int128" {
						test.s = "take(int128(['e1671797c52e15f763380b45e841ec32']), 70000)"
					}
				}
				res, err := db.RunScript(test.s)
				So(err, ShouldBeNil)
				vec := res.(*model.Vector)
				So(err, ShouldBeNil)
				So(vec.GetDataType(), ShouldEqual, test.ex_type)
				So(vec.Data.Len(), ShouldEqual, 70000)
				for i := 0; i < 70000; i++ {
					assert.Equal(t, vec.Get(i).String(), test.ex_val)
				}

			})
			Convey(fmt.Sprintf("test upload vector with type %s", test.typestr), func() {
				if test.s == "default" {
					test.s = fmt.Sprintf("ex = array(%s, 70000,, 1);ex", strings.ToUpper(test.typestr))
				} else {
					if test.typestr == "string" || test.typestr == "symbol" {
						test.s = fmt.Sprintf("ex = array(%s, 70000,, 'abc');ex", strings.ToUpper(test.typestr))
					} else if test.typestr == "blob" {
						test.s = "ex=take(blob(['abc']), 70000);ex"
					} else if test.typestr == "uuid" {
						test.s = "ex=take(uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87']), 70000);ex"
					} else if test.typestr == "ipaddr" {
						test.s = "ex=take(ipaddr(['1.1.1.1']), 70000);ex"
					} else if test.typestr == "int128" {
						test.s = "ex=take(int128(['e1671797c52e15f763380b45e841ec32']), 70000);ex"
					}
				}
				res, err := db.RunScript(test.s)
				So(err, ShouldBeNil)
				vec := res.(*model.Vector)
				So(err, ShouldBeNil)
				_, err = db.Upload(map[string]model.DataForm{"vec": vec})
				So(err, ShouldBeNil)
				res, err = db.RunScript("eqObj(ex, vec)")
				So(err, ShouldBeNil)
				assert.True(t, res.(*model.Scalar).Value().(bool))

			})

		}
	})
}
