package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Set_DownLoad_DataType_int(t *testing.T) {
	t.Parallel()
	Convey("Test_set_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_int_not_null:", func() {
			s, err := db.RunScript("a=set(4 5 5 2 3 11 6);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			zx := []int32{6, 11, 3, 2, 5, 4}
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 6)
		})
		Convey("Test_set_int_has_null:", func() {
			s, err := db.RunScript("a = set(1024 12 -30 15 NULL  2);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []int32{1024, 12, -30, 15, model.NullInt, 2}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 6)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 6)
		})
		Convey("Test_set_int_all_null:", func() {
			s, err := db.RunScript("a=take(00i,6);b=set(a);b")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullInt)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		Convey("Test_set_int_has_same_element:", func() {
			s, err := db.RunScript("a = set(1024 12 -30 15 1024  2);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []int32{2, 15, -30, 12, 1024}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 5)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 5)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_string(t *testing.T) {
	t.Parallel()
	Convey("Test_set_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_string_not_null:", func() {
			s, err := db.RunScript("a=set('trs1' 'fal' 'rue' 'else');a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"else", "rue", "fal", "trs1"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_string_has_null:", func() {
			s, err := db.RunScript("a=set('trs1' '' 'rue' 'else');a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"else", "rue", "", "trs1"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_string_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a=set('' '' '' '');a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{""}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_char(t *testing.T) {
	t.Parallel()
	Convey("Test_set_char:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_char_not_null:", func() {
			s, err := db.RunScript("a=set(1c 25c 97c 124c);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			So(re[0], ShouldEqual, 'a')
			So(re[0], ShouldEqual, 97)
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_char_has_null:", func() {
			s, err := db.RunScript("a=set(1c 25c NULL 124c);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			So(result.Vector.Data.IsNull(0), ShouldBeTrue)
			So(re[1], ShouldEqual, 124)
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_char_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a=set(char['','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			for j := 0; j < result.Rows(); j++ {
				So(result.Vector.Data.IsNull(j), ShouldBeTrue)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_short(t *testing.T) {
	t.Parallel()
	Convey("Test_set_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_short_not_null:", func() {
			s, err := db.RunScript("a=set(-2h 25h -917h 1024h);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []int16{1024, -917, 25, -2}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_short_has_null:", func() {
			s, err := db.RunScript("a=set(-2h NULL -917h 1024h);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []int16{1024, -917, model.NullShort, -2}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_short_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a=set(take(00h,6));a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullShort)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_long(t *testing.T) {
	t.Parallel()
	Convey("Test_set_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_long_not_null:", func() {
			s, err := db.RunScript("a=set(-2l 25l -917l 1024l);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []int64{1024, -917, 25, -2}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_long_has_null:", func() {
			s, err := db.RunScript("a=set(-2l NULL -917l 1024l);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []int64{1024, -917, model.NullLong, -2}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_long_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a=set(take(00l,6));a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullLong)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_double(t *testing.T) {
	t.Parallel()
	Convey("Test_set_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_double_not_null:", func() {
			s, err := db.RunScript("a=set(-2.0 25.0 -917.0 1024.0);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []float64{1024.0, -917.0, 25.0, -2.0}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_double_has_null:", func() {
			s, err := db.RunScript("a=set(-2.0 NULL -917.0 1024.0);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []float64{1024, -917, model.NullDouble, -2}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_double_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a=set(double['','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullDouble)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_float(t *testing.T) {
	t.Parallel()
	Convey("Test_set_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_float_not_null:", func() {
			s, err := db.RunScript("a=set(-2.0f 25.0f -917.0f 1024.0f);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []float32{1024.0, -917.0, 25.0, -2.0}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_float_has_null:", func() {
			s, err := db.RunScript("a=set(-2.0f NULL -917.0f 1024.0f);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []float32{1024.0, -917.0, model.NullFloat, -2.0}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_float_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a=set(take(00f,6));a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullFloat)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_date(t *testing.T) {
	t.Parallel()
	Convey("Test_set_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_date_not_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 2006.01.02 1970.01.01 2006.01.03);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_date_has_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 NULL 1970.01.01 2006.01.03);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_date_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(take(00d,4));a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_month(t *testing.T) {
	t.Parallel()
	Convey("Test_set_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_month:", func() {
			s, err := db.RunScript("a = set(1969.12M 2006.01M 1970.01M 2006.02M);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 2, 1, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_month_has_null:", func() {
			s, err := db.RunScript("a = set(1969.12M NULL 1970.01M 2006.02M);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 2, 1, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_month_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(month['','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_time(t *testing.T) {
	t.Parallel()
	Convey("Test_set_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_time_not_null:", func() {
			s, err := db.RunScript("a = set(23:59:59.999 00:00:01.000 09:11:25.000 11:22:33.000);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1970, 1, 1, 9, 11, 25, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_time_has_null:", func() {
			s, err := db.RunScript("a = set(23:59:59.999 00:00:01.000 NULL 11:22:33.000);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_time_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(time['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_minute(t *testing.T) {
	t.Parallel()
	Convey("Test_set_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_minute_not_null:", func() {
			s, err := db.RunScript("a = set(23:59m 00:00m 09:11m 11:22m);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 9, 11, 0, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_minute_has_null:", func() {
			s, err := db.RunScript("a = set(23:59m 00:00m NULL 11:22m);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_minute_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(minute['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_second(t *testing.T) {
	t.Parallel()
	Convey("Test_set_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_second_not_null:", func() {
			s, err := db.RunScript("a = set(23:59:59 00:00:00 09:11:59 11:22:33);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 9, 11, 59, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_second_has_null:", func() {
			s, err := db.RunScript("a = set(23:59:59 00:00:00 NULL 11:22:33);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_second_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(take(00s,6));a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_datetime(t *testing.T) {
	t.Parallel()
	Convey("Test_set_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_datetime_not_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 23:59:59 2006.01.02 15:04:04 1970.01.01 00:00:00 2006.01.03 15:04:05);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 4, 5, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_datetime_has_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 23:59:59 2006.01.02 15:04:04 NULL 2006.01.03 15:04:05);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 4, 5, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_datetime_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(datetime['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_timestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_set_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_timestamp_not_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 23:59:59.999 2006.01.02 15:04:04.999 1970.01.01 00:00:00.000 2006.01.03 15:04:05.999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 4, 5, 999000000, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_timestamp_has_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 23:59:59.999 2006.01.02 15:04:04.999 NULL 2006.01.03 15:04:05.999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 4, 5, 999000000, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_timestamp_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(timestamp['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_nanotime(t *testing.T) {
	t.Parallel()
	Convey("Test_set_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_nanotime_not_null:", func() {
			s, err := db.RunScript("a = set(23:59:59.999999999 00:00:00.000000000 09:11:59.999999999 11:22:33.445566778);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 9, 11, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 11, 22, 33, 445566778, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_nanotime_has_null:", func() {
			s, err := db.RunScript("a = set(23:59:59.999999999 00:00:00.000000000 NULL 11:22:33.445566778);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 11, 22, 33, 445566778, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_nanotime_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(nanotime['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_nanotimestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_set_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_nanotimestamp_not_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 23:59:59.999999999 2006.01.02 15:04:04.999999999 1970.01.01 00:00:00.000000000 2006.01.03 15:04:05.999999999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 4, 5, 999999999, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_nanotimestamp_has_null:", func() {
			s, err := db.RunScript("a = set(1969.12.31 23:59:59.999999999 2006.01.02 15:04:04.999999999 NULL 2006.01.03 15:04:05.999999999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 4, 5, 999999999, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_nanotimestamp_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(nanotimestamp['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_datehour(t *testing.T) {
	t.Parallel()
	Convey("Test_set_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_datehour_not_null:", func() {
			s, err := db.RunScript("a = set(datehour[1969.12.31 23:14:11,2006.01.02 15:15:11,1970.01.01 00:16:11,2006.01.03 15:17:11]);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datehour")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_datehour_has_null:", func() {
			s, err := db.RunScript("a = set(datehour[1969.12.31 23:14:11,2006.01.02 15:15:11,,2006.01.03 15:17:11]);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 3, 15, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datehour")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_datehour_all_null:", func() {
			s, err := db.RunScript("a = set(datehour['','','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datehour")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_uuid(t *testing.T) {
	t.Parallel()
	Convey("Test_set_uuid:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_uuid_not_null:", func() {
			s, err := db.RunScript("a = set(uuid['cd468f9a-1834-1cf5-62b6-26270a9b5d55','c7deab2a-26f0-533d-395d-2b1c3f93116b','dc62fba4-570a-c08e-f175-68744cec24b4','6dea85d7-0e44-8eee-0baa-8ca5eb298338']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"6dea85d7-0e44-8eee-0baa-8ca5eb298338", "dc62fba4-570a-c08e-f175-68744cec24b4", "c7deab2a-26f0-533d-395d-2b1c3f93116b", "cd468f9a-1834-1cf5-62b6-26270a9b5d55"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_uuid_has_null:", func() {
			s, err := db.RunScript("a = set(uuid['cd468f9a-1834-1cf5-62b6-26270a9b5d55','c7deab2a-26f0-533d-395d-2b1c3f93116b','dc62fba4-570a-c08e-f175-68744cec24b4','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{model.NullUUID, "dc62fba4-570a-c08e-f175-68744cec24b4", "c7deab2a-26f0-533d-395d-2b1c3f93116b", "cd468f9a-1834-1cf5-62b6-26270a9b5d55"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_uuid_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(uuid['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullUUID)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_ipaddr(t *testing.T) {
	t.Parallel()
	Convey("Test_set_ipaddr:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_ipaddr_not_null:", func() {
			s, err := db.RunScript("a = set(ipaddr['2a35:5753:12c4:e705:a700:8507:a36e:cd23','1d8:2691:125e:cdaa:2d57:7cdf:428e:f4e5','ad1:4e1e:5961:b56b:8521:9a40:fefc:89ef','40ce:6bf6:af3d:f8f:d4f8:8cef:5d37:2af3']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"40ce:6bf6:af3d:f8f:d4f8:8cef:5d37:2af3", "ad1:4e1e:5961:b56b:8521:9a40:fefc:89ef", "1d8:2691:125e:cdaa:2d57:7cdf:428e:f4e5", "2a35:5753:12c4:e705:a700:8507:a36e:cd23"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "ipaddr")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_ipaddr_has_null:", func() {
			s, err := db.RunScript("a = set(ipaddr['2a35:5753:12c4:e705:a700:8507:a36e:cd23','','ad1:4e1e:5961:b56b:8521:9a40:fefc:89ef','40ce:6bf6:af3d:f8f:d4f8:8cef:5d37:2af3']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"40ce:6bf6:af3d:f8f:d4f8:8cef:5d37:2af3", "ad1:4e1e:5961:b56b:8521:9a40:fefc:89ef", model.NullIP, "2a35:5753:12c4:e705:a700:8507:a36e:cd23"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "ipaddr")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_ipaddr_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(ipaddr['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullIP)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "ipaddr")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_int128(t *testing.T) {
	t.Parallel()
	Convey("Test_set_int128:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_int128_not_null:", func() {
			s, err := db.RunScript("a = set(int128['c822209cea11798e2e8db17fa5e95d13','b6a5a2586bf064736c80a591a423b98d','84693a7c085a6b2842d8db207ea7c000','e3ddb9ec3328d2908f666dc0fed0a6bc']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"e3ddb9ec3328d2908f666dc0fed0a6bc", "84693a7c085a6b2842d8db207ea7c000", "b6a5a2586bf064736c80a591a423b98d", "c822209cea11798e2e8db17fa5e95d13"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_int128_has_null:", func() {
			s, err := db.RunScript("a = set(int128['c822209cea11798e2e8db17fa5e95d13','','84693a7c085a6b2842d8db207ea7c000','e3ddb9ec3328d2908f666dc0fed0a6bc']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			zx := []string{"e3ddb9ec3328d2908f666dc0fed0a6bc", "84693a7c085a6b2842d8db207ea7c000", model.NullInt128, "c822209cea11798e2e8db17fa5e95d13"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(result.Vector.RowCount, ShouldEqual, 4)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 4)
		})
		Convey("Test_set_int128_all_null/has_same_ele:", func() {
			s, err := db.RunScript("a = set(int128['','','','','','']);a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			re := result.Vector.Data.Value()
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, model.NullInt128)
			}
			So(result.Vector.RowCount, ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
			form := result.GetDataForm()
			So(form, ShouldEqual, 4)
			row := result.Rows()
			So(row, ShouldEqual, 1)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_DownLoad_DataType_set_be_cleared(t *testing.T) {
	t.Parallel()
	Convey("Test_set_be_cleaned:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_be_cleared:", func() {
			s, err := db.RunScript("a=set(int);a.clear!();a")
			So(err, ShouldBeNil)
			result := s.(*model.Set)
			So(result.Vector.RowCount, ShouldEqual, 0)
			So(result.Vector.ColumnCount, ShouldEqual, 1)
			So(result.GetDataTypeString(), ShouldEqual, "int")
			So(result.GetDataForm(), ShouldEqual, model.DfSet)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_int(t *testing.T) {
	t.Parallel()
	Convey("Test_set_int_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_int:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []int32{1, 2, 3, 4, 5}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(INT SET)")
			So(res.GetDataType(), ShouldEqual, model.DtInt)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_short(t *testing.T) {
	t.Parallel()
	Convey("Test_set_short_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_setshort:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtShort, []int16{1, 2, 3, 4, 5, 6, 7, 8, 9})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []int16{1, 2, 3, 4, 5, 6, 7, 8, 9}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(SHORT SET)")
			So(res.GetDataType(), ShouldEqual, model.DtShort)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_char(t *testing.T) {
	t.Parallel()
	Convey("Test_set_char_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_char:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtChar, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}
			So(re, ShouldNotBeIn, zx)
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(CHAR SET)")
			So(res.GetDataType(), ShouldEqual, model.DtChar)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_long(t *testing.T) {
	t.Parallel()
	Convey("Test_set_long_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_long:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtLong, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(LONG SET)")
			So(res.GetDataType(), ShouldEqual, model.DtLong)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_float(t *testing.T) {
	t.Parallel()
	Convey("Test_set_float_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_short:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtFloat, []float32{1, 2, 3, 4, 5, 6, 7, 8, 9})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []float32{1, 2, 3, 4, 5, 6, 7, 8, 9}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FLOAT SET)")
			So(res.GetDataType(), ShouldEqual, model.DtFloat)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_double(t *testing.T) {
	t.Parallel()
	Convey("Test_set_double_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_double:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtDouble, []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, 7, 8, 9})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []float64{1024.2, -2.10, 36897542.233, -5454545454, 8989.12125, 6, 7, 8, 9}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DOUBLE SET)")
			So(res.GetDataType(), ShouldEqual, model.DtDouble)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_date(t *testing.T) {
	t.Parallel()
	Convey("Test_set_date_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_date:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DATE SET)")
			So(res.GetDataType(), ShouldEqual, model.DtDate)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_month(t *testing.T) {
	t.Parallel()
	Convey("Test_set_month_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_month:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtMonth, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(MONTH SET)")
			So(res.GetDataType(), ShouldEqual, model.DtMonth)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_time(t *testing.T) {
	t.Parallel()
	Convey("Test_set_time_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_time:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(TIME SET)")
			So(res.GetDataType(), ShouldEqual, model.DtTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_minute(t *testing.T) {
	t.Parallel()
	Convey("Test_set_minute_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_minute:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtMinute, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(MINUTE SET)")
			So(res.GetDataType(), ShouldEqual, model.DtMinute)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_second(t *testing.T) {
	t.Parallel()
	Convey("Test_set_second_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_second:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtSecond, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(SECOND SET)")
			So(res.GetDataType(), ShouldEqual, model.DtSecond)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_datetime(t *testing.T) {
	t.Parallel()
	Convey("Test_set_datetime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_datetime:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtDatetime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DATETIME SET)")
			So(res.GetDataType(), ShouldEqual, model.DtDatetime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_timestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_set_timestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_timestamp:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(TIMESTAMP SET)")
			So(res.GetDataType(), ShouldEqual, model.DtTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_nanotime(t *testing.T) {
	t.Parallel()
	Convey("Test_set_nanotime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_nanotime:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtNanoTime, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(NANOTIME SET)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_nanotimestamp(t *testing.T) {
	t.Parallel()
	Convey("Test_set_nanotimestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_nanotimestamp:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtNanoTimestamp, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(NANOTIMESTAMP SET)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_datehour(t *testing.T) {
	t.Parallel()
	Convey("Test_set_datehour_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_datehour:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtDateHour, []time.Time{time.Date(2022, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			time1 := []time.Time{time.Date(2022, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, time1)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DATEHOUR SET)")
			So(res.GetDataType(), ShouldEqual, model.DtDateHour)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_point(t *testing.T) {
	t.Parallel()
	Convey("Test_set_point_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_point:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtPoint, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []string{"(1.00000, 1.00000)", "(-1.00000, -1024.50000)", "(1001022.40000, -30028.75000)"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(POINT SET)")
			So(res.GetDataType(), ShouldEqual, model.DtPoint)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_complex(t *testing.T) {
	t.Parallel()
	Convey("Test_set_complex_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_complex:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtComplex, [][2]float64{{1, 1}, {-1, -1024.5}, {1001022.4, -30028.75}})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []string{"1.00000+1.00000i", "-1.00000+-1024.50000i", "1001022.40000+-30028.75000i"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(COMPLEX SET)")
			So(res.GetDataType(), ShouldEqual, model.DtComplex)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_string(t *testing.T) {
	t.Parallel()
	Convey("Test_set_string_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_string:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtString, []string{"hello", "#$%", "数据类型", "what"})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []string{"hello", "#$%", "数据类型", "what"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(STRING SET)")
			So(res.GetDataType(), ShouldEqual, model.DtString)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_blob(t *testing.T) {
	t.Parallel()
	Convey("Test_set_blob_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_blob:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtBlob, [][]byte{{6}, {12}, {56}, {128}})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := [][]uint8{{6}, {12}, {56}, {128}}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(BLOB SET)")
			So(res.GetDataType(), ShouldEqual, model.DtBlob)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_uuid(t *testing.T) {
	t.Parallel()
	Convey("Test_set_uuid_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_uuid:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88", "5d212a78-cc48-e3b1-4235-b4d91473ee89"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(UUID SET)")
			So(res.GetDataType(), ShouldEqual, model.DtUUID)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_ipaddr(t *testing.T) {
	t.Parallel()
	Convey("Test_set_ipaddr_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_ipaddr:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtIP, []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []string{"192.163.1.12", "0.0.0.0", "127.0.0.1"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(IPADDR SET)")
			So(res.GetDataType(), ShouldEqual, model.DtIP)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_int128(t *testing.T) {
	t.Parallel()
	Convey("Test_set_int128_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_int128:", func() {
			data, _ := model.NewDataTypeListFromRawData(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"})
			set := model.NewSet(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": set})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Set).Vector.Data.Value()
			zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33", "e1671797c52e15f763380b45e841ec34"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldBeIn, zx)
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(INT128 SET)")
			So(res.GetDataType(), ShouldEqual, model.DtInt128)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Set_UpLoad_DataType_big_array(t *testing.T) {
	t.Parallel()
	Convey("Test_set_big_array_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		var i int32
		intv := []int32{}
		for i = 0; i < 3000000*12; i += 12 {
			intv = append(intv, i)
		}
		intv = append(intv, model.NullInt)
		col, err := model.NewDataTypeListFromRawData(model.DtInt, intv)
		So(err, ShouldBeNil)
		set := model.NewSet(model.NewVector(col))
		_, err = db.Upload(map[string]model.DataForm{"s": set})
		So(err, ShouldBeNil)
		res, _ := db.RunScript("s")
		ty, _ := db.RunScript("typestr(s)")
		re := res.(*model.Set)
		So(re.Vector.ColumnCount, ShouldEqual, 1)
		So(re.Vector.RowCount, ShouldEqual, 3000001)
		So(ty.String(), ShouldEqual, "string(INT SET)")
		So(res.GetDataType(), ShouldEqual, model.DtInt)
		So(db.Close(), ShouldBeNil)
	})
}

func Test_Set_huge_val(t *testing.T) {
	t.Parallel()
	Convey("Test_set_big_string_symbol_blob:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_set_big_string:", func() {
			res, _ := db.RunScript("a = array(STRING,10).append!(string(concat(take(\"123&#@!^%;d《》中文\",100000))));set(a)")

			result := res.(*model.Set)

			So(result.Vector.RowCount, ShouldEqual, 2)
			// fmt.Println(result.Vector.String())
			So(result.Vector.Get(0).Value(), ShouldEqual, strings.Repeat("123&#@!^%;d《》中文", 100000))
			So(result.Vector.Get(1).Value(), ShouldEqual, "")
			_, err := db.Upload(map[string]model.DataForm{"asd": result})
			So(err.Error(), ShouldContainSubstring, "Serialized string length must less than 256k bytes.")
		})
		Convey("Test_set_big_blob:", func() {
			res, _ := db.RunScript("a = array(BLOB,10).append!(blob(concat(take(\"123&#@!^%;d《》中文\",100000))));set(a)")

			result := res.(*model.Set)

			So(result.Vector.RowCount, ShouldEqual, 2)
			// fmt.Println(result.Vector.String())
			So(result.Vector.Get(0).String(), ShouldEqual, strings.Repeat("123&#@!^%;d《》中文", 100000))
			So(result.Vector.Get(1).Value(), ShouldResemble, []uint8(nil))
			db.Upload(map[string]model.DataForm{"adg": result})
			_, err := db.RunScript("assert 1, blob(concat(take(\"123&#@!^%;d《》中文\",100000))) in adg;assert 2, NULL in adg")
			So(err, ShouldBeNil)
		})
		Convey("Test_set_big_symbol:", func() {
			res, err := db.RunScript("a = array(SYMBOL,10).append!(symbol([concat(take(\"123&#@!^%;d《》中文\",100000))])[0]);set(a)")
			So(err, ShouldBeNil)
			result := res.(*model.Set)
			So(result.Vector.Get(0).String(), ShouldEqual, strings.Repeat("123&#@!^%;d《》中文", 100000))
			So(result.Vector.Get(1).String(), ShouldEqual, "")
			var str string
			for i := 0; i < 300000; i++ {
				str += "a"
			}
			rawdata, _ := model.NewDataTypeListFromRawData(model.DtSymbol, []string{str, str + "123"})
			s := model.NewVector(rawdata)
			p := model.NewSet(s)
			_, err = db.Upload(map[string]model.DataForm{"p": p})
			So(err.Error(), ShouldContainSubstring, "Serialized string length must less than 256k bytes.")
		})

		So(db.Close(), ShouldBeNil)
	})
}
