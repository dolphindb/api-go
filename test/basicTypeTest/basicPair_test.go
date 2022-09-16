package test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Pair_DownLoad_int(t *testing.T) {
	Convey("Test_pair_int:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_int_not_null:", func() {
			s, err := db.RunScript("a=(-1024:1048576);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -1024)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1048576)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
			form := result.GetDataForm()
			So(form, ShouldEqual, 2)
			row := result.Rows()
			So(row, ShouldEqual, 2)
		})
		Convey("Test_pair_int_pre_one_nll:", func() {
			s, err := db.RunScript("a=(:1048576);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1048576)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		Convey("Test_pair_int_last_one_nll:", func() {
			s, err := db.RunScript("a=(1:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, 1)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 4)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_string(t *testing.T) {
	Convey("Test_pair_string:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_string_not_null:", func() {
			s, err := db.RunScript("a=(`hello:`world);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, "hello")
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, "world")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
		})
		Convey("Test_pair_string_pre_one_null:", func() {
			s, err := db.RunScript("a=(`:`theworld);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, "theworld")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
		})
		Convey("Test_pair_string_last_one_null:", func() {
			s, err := db.RunScript("a=(`thehello :`);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, "thehello")
			reType := result.GetDataType()
			So(reType, ShouldEqual, 18)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "string")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_char(t *testing.T) {
	Convey("Test_pair_char:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_char_not_null:", func() {
			s, err := db.RunScript("a=(1c:124c);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, 1)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 124)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
		})
		Convey("Test_pair_char_pre_one_null:", func() {
			s, err := db.RunScript("a=( :124c);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 124)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
		})
		Convey("Test_pair_char_last_one_null:", func() {
			s, err := db.RunScript("a=(16c:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, 16)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 2)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "char")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_bool(t *testing.T) {
	Convey("Test_pair_bool:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_bool_not_null:", func() {
			s, err := db.RunScript("a=(true:false);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, true)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, false)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 1)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "bool")
		})
		Convey("Test_pair_bool_pre_one_null:", func() {
			s, err := db.RunScript("a=(:false);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, false)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 1)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "bool")
		})
		Convey("Test_pair_bool_pair_last_one_null:", func() {
			s, err := db.RunScript("a=(true :);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, true)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 1)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "bool")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_short(t *testing.T) {
	Convey("Test_pair_short:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_short:", func() {
			s, err := db.RunScript("a=(-2h:1024h);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -2)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1024)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		Convey("Test_pair_short_pre_one_null:", func() {
			s, err := db.RunScript("a=( :31689h);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 31689)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
		})
		Convey("Test_pair_short_last_one_null:", func() {
			s, err := db.RunScript("a=(-15225h:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -15225)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 3)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "short")
			by := bytes.NewBufferString("")
			w := protocol.NewWriter(by)
			err = result.Render(w, protocol.LittleEndian)
			So(err, ShouldBeNil)
			w.Flush()
			by.Reset()
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_long(t *testing.T) {
	Convey("Test_pair_long:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_long_not_null:", func() {
			s, err := db.RunScript("a=(-2l:1024l);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -2)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1024)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		Convey("Test_pair_long_pre_one_null:", func() {
			s, err := db.RunScript("a=(:1048576l);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1048576)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		Convey("Test_pair_long_last_one_null:", func() {
			s, err := db.RunScript("a=(-1048576l:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -1048576)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 5)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "long")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_double(t *testing.T) {
	Convey("Test_pair_double:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_double_not_null:", func() {
			s, err := db.RunScript("a=(-2.0:1048576.0);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -2.0)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1048576)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		Convey("Test_pair_double_pre_one_null:", func() {
			s, err := db.RunScript("a=(:-1048576.0);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, -1048576)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		Convey("Test_pair_double_last_one_null:", func() {
			s, err := db.RunScript("a=(1024.0:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, 1024)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 16)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "double")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_float(t *testing.T) {
	Convey("Test_pair_float:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_float_not_null:", func() {
			s, err := db.RunScript("a=(-2.0f:1048576.0f);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -2.0)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, 1048576.0)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		Convey("Test_pair_float_pre_one_null:", func() {
			s, err := db.RunScript("a=(:-1048576.0f);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, -1048576.0)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		Convey("Test_pair_float_last_one_null:", func() {
			s, err := db.RunScript("a=(-1024.0f:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, -1024)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 15)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "float")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_date(t *testing.T) {
	Convey("Test_pair_date:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_date_not_null:", func() {
			s, err := db.RunScript("a=(1969.12.31:2006.01.02);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		Convey("Test_pair_date_pre_one_null:", func() {
			s, err := db.RunScript("a=(:2006.01.02);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		Convey("Test_pair_last_pre_one_null:", func() {
			s, err := db.RunScript("a=(1969.12.31:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 6)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "date")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_month(t *testing.T) {
	Convey("Test_pair_month:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_month_not_null:", func() {
			s, err := db.RunScript("a=(1969.12M:2006.01M);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_pair_month_pre_one_null:", func() {
			s, err := db.RunScript("a=(:2006.01M);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		Convey("Test_pair_month_last_one_null:", func() {
			s, err := db.RunScript("a=(1969.12M:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 7)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "month")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_time(t *testing.T) {
	Convey("Test_pair_time:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_time_not_null:", func() {
			s, err := db.RunScript("a=(11:11:11.000:12:12:12.222);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 11, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 12, 222000000, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		Convey("Test_pair_time_pre_one_null:", func() {
			s, err := db.RunScript("a=(:12:12:12.222);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 11, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 12, 222000000, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		Convey("Test_pair_time_last_one_null:", func() {
			s, err := db.RunScript("a=(11:11:11.000:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 11, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 12, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 8)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "time")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_minute(t *testing.T) {
	Convey("Test_pair_minute:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_minute_not_null:", func() {
			s, err := db.RunScript("a=(11:11m:12:12m);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		Convey("Test_pair_minute_pre_one_null:", func() {
			s, err := db.RunScript("a=( :12:12m);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		Convey("Test_pair_minute_last_one_null:", func() {
			s, err := db.RunScript("a=(11:11m:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 0, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 9)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "minute")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_second(t *testing.T) {
	Convey("Test_pair_second:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_second_not_null:", func() {
			s, err := db.RunScript("a=(11:11:11:12:12:12);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 11, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 12, 0, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		Convey("Test_pair_second_pre_one_null:", func() {
			s, err := db.RunScript("a=(:12:12:12);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 11, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 12, 0, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		Convey("Test_pair_second_last_one_null:", func() {
			s, err := db.RunScript("a=(11:11:11:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 11, 11, 11, 0, time.UTC), time.Date(1970, 1, 1, 12, 12, 12, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 10)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "second")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_datetime(t *testing.T) {
	Convey("Test_pair_datetime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_datetime_not_null:", func() {
			s, err := db.RunScript("a=(1969.12.31 23:59:59:2006.01.02 15:04:04);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		Convey("Test_pair_datetime_pre_one_null:", func() {
			s, err := db.RunScript("a=(:2006.01.02 15:04:04);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		Convey("Test_pair_datetime_last_one_null:", func() {
			s, err := db.RunScript("a=(1969.12.31 23:59:59:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 11)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "datetime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_timestamp(t *testing.T) {
	Convey("Test_pair_timestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_timestamp_not_null:", func() {
			s, err := db.RunScript("a=(1969.12.31 23:59:59.999:2006.01.02 15:04:04.999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		Convey("Test_pair_timestamp_pre_one_null:", func() {
			s, err := db.RunScript("a=(:2006.01.02 15:04:04.999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		Convey("Test_pair_timestamp_last_one_null:", func() {
			s, err := db.RunScript("a=(1969.12.31 23:59:59.999:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999000000, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 12)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "timestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_nanotime(t *testing.T) {
	Convey("Test_pair_nanotime:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_nanotime_not_null:", func() {
			s, err := db.RunScript("a=(23:59:59.999999999:15:04:04.999999999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		Convey("Test_pair_nanotime_pre_one_null:", func() {
			s, err := db.RunScript("a=(:15:04:04.999999999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		Convey("Test_pair_nanotime_last_one_null:", func() {
			s, err := db.RunScript("a=(23:59:59.999999999:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 13)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotime")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_nanotimestamp(t *testing.T) {
	Convey("Test_pair_nanotimestamp:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_nanotimestamp_not_null:", func() {
			s, err := db.RunScript("a=(1969.12.31 23:59:59.999999999:2006.01.02 15:04:04.999999999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		Convey("Test_pair_nanotimestamp_pre_one_null:", func() {
			s, err := db.RunScript("a=(:2006.01.02 15:04:04.999999999);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		Convey("Test_pair_nanotimestamp_last_one_null :", func() {
			s, err := db.RunScript("a=(1969.12.31 23:59:59.999999999:);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 14)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "nanotimestamp")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_datehour(t *testing.T) {
	Convey("Test_pair_datehour:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_datehour_not_null:", func() {
			s, err := db.RunScript("a=(datehour(1969.12.31 23:59:59.999999999):datehour(2006.01.02 15:04:04.999999999));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		Convey("Test_pair_datehour_pre_one_null :", func() {
			s, err := db.RunScript("a=(:datehour(2006.01.02 15:04:04.999999999));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		Convey("Test_pair_datehour_last_one_null:", func() {
			s, err := db.RunScript("a=(datehour(1969.12.31 23:59:59.999999999):);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 28)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "dateHour")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_uuid(t *testing.T) {
	Convey("Test_pair_uuid:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_uuid_not_null:", func() {
			s, err := db.RunScript("a = (uuid('e5345c41-da6d-d400-1b5a-6ca6e8a52ec0'):uuid('f521c024-3a1d-b043-fb68-822b8ba047a8'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"e5345c41-da6d-d400-1b5a-6ca6e8a52ec0", "f521c024-3a1d-b043-fb68-822b8ba047a8"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
		})
		Convey("Test_pair_uuid_pre_one_null:", func() {
			s, err := db.RunScript("a = (:uuid('f521c024-3a1d-b043-fb68-822b8ba047a8'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"e5345c41-da6d-d400-1b5a-6ca6e8a52ec0", "f521c024-3a1d-b043-fb68-822b8ba047a8"}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
		})
		Convey("Test_pair_uuid_last_one_null:", func() {
			s, err := db.RunScript("a = (uuid('e5345c41-da6d-d400-1b5a-6ca6e8a52ec0'):);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"e5345c41-da6d-d400-1b5a-6ca6e8a52ec0", "f521c024-3a1d-b043-fb68-822b8ba047a8"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 19)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "uuid")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_iapaddr(t *testing.T) {
	Convey("Test_pair_ipaddr:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_ipaddr_not_null:", func() {
			s, err := db.RunScript("a = (ipaddr('461c:7fa1:7f3c:7249:5278:c610:f595:d174'):ipaddr('3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"461c:7fa1:7f3c:7249:5278:c610:f595:d174", "3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		Convey("Test_pair_ipaddr number:", func() {
			s, err := db.RunScript("a = (ipaddr('192.13.1.33'):ipaddr('191.168.1.13'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"192.13.1.33", "191.168.1.13"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		Convey("Test_pair_ipaddr_pre_one_null:", func() {
			s, err := db.RunScript("a = (:ipaddr('3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"461c:7fa1:7f3c:7249:5278:c610:f595:d174", "3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72"}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		Convey("Test_pair_ipaddr_last_one_null:", func() {
			s, err := db.RunScript("a = (ipaddr('461c:7fa1:7f3c:7249:5278:c610:f595:d174'):);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"461c:7fa1:7f3c:7249:5278:c610:f595:d174", "3de8:13c6:df5f:bcd5:7605:3827:e37a:3a72"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 30)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "IP")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_int128(t *testing.T) {
	Convey("Test_pair_int128:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_int128_not_null:", func() {
			s, err := db.RunScript("a = (int128('e1671797c52e15f763380b45e841ec32'):int128('e1671797c52e15f763380b45e841ec33'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
		})
		Convey("Test_pair_int128_pre_one_null:", func() {
			s, err := db.RunScript("a = (:int128('e1671797c52e15f763380b45e841ec33'));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33"}
			So(result.Vector.Data.Get(0).IsNull(), ShouldBeTrue)
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
		})
		Convey("Test_pair_int128_last_one_null:", func() {
			s, err := db.RunScript("a = (int128('e1671797c52e15f763380b45e841ec32'):);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).IsNull(), ShouldBeTrue)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 31)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "int128")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_point(t *testing.T) {
	Convey("Test_pair_point:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_point_not_null:", func() {
			s, err := db.RunScript("a = (point(2,3):point(5,2));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"(2.00000, 3.00000)", "(5.00000, 2.00000)"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 35)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "point")
		})
		Convey("Test_pair_point_pre_one_null:", func() {
			s, err := db.RunScript("a = (:point(5,2));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"(0.00000, 0.00000)", "(5.00000, 2.00000)"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 35)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "point")
		})
		Convey("Test_pair_point_last_one_null:", func() {
			s, err := db.RunScript("a = (point(2,3):);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			fmt.Print(result)
			zx := []string{"(2.00000, 3.00000)", "(0.00000, 0.00000)"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 35)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "point")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_complex(t *testing.T) {
	Convey("Test_pair_complex:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_complex_not_null:", func() {
			s, err := db.RunScript("a = (complex(2,3):complex(5,2));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"2.00000+3.00000i", "5.00000+2.00000i"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 34)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "complex")
		})
		Convey("Test_pair_point_pre_one_null:", func() {
			s, err := db.RunScript("a = (:complex(5,2));a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"0.00000+0.00000i", "5.00000+2.00000i"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 34)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "complex")
		})
		Convey("Test_pair_point_last_one_null:", func() {
			s, err := db.RunScript("a = (complex(2,3):);a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"2.00000+3.00000i", "0.00000+0.00000i"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 34)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "complex")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_DownLoad_duration(t *testing.T) {
	Convey("Test_pair_duration:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_duration_not_null:", func() {
			s, err := db.RunScript("a = 1H:1s;a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"1H", "1s"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 36)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "duration")
		})
		Convey("Test_pair_duration_pre_one_null:", func() {
			s, err := db.RunScript("a = :1s;a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"0", "1s"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 36)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "duration")
		})
		Convey("Test_pair_duration_last_one_null:", func() {
			s, err := db.RunScript("a = 1H:;a")
			So(err, ShouldBeNil)
			result := s.(*model.Pair)
			zx := []string{"1H", "0"}
			So(result.Vector.Data.Get(0).Value(), ShouldEqual, zx[0])
			So(result.Vector.Data.Get(1).Value(), ShouldEqual, zx[1])
			reType := result.GetDataType()
			So(reType, ShouldEqual, 36)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "duration")
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_int(t *testing.T) {
	Convey("Test_pair_int_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_int:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtInt, []int32{-211, 9984})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []int32{-211, 9984}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(INT PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtInt)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_short(t *testing.T) {
	Convey("Test_pair_short_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_short:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtShort, []int16{-211, 9984})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []int16{-211, 9984}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(SHORT PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtShort)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_char(t *testing.T) {
	Convey("Test_pair_char_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_char:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtChar, []uint8{127, 84})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []uint8{127, 84}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(CHAR PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtChar)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_long(t *testing.T) {
	Convey("Test_pair_long_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_long:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtLong, []int64{1212457, -21655484})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []int64{1212457, -21655484}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(LONG PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtLong)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_float(t *testing.T) {
	Convey("Test_pair_float_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_float:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtFloat, []float32{1212.457, -216.55484})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []float32{1212.457, -216.55484}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(FLOAT PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtFloat)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_double(t *testing.T) {
	Convey("Test_pair_double_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_double:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtDouble, []float64{1212.457, -216.55484})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []float64{1212.457, -216.55484}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DOUBLE PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtDouble)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_date(t *testing.T) {
	Convey("Test_pair_date_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_date:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtDate, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DATE PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtDate)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_month(t *testing.T) {
	Convey("Test_pair_month_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_month:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtMonth, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(MONTH PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtMonth)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_time(t *testing.T) {
	Convey("Test_pair_time_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_time:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtTime, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999000000, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999000000, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(TIME PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_minute(t *testing.T) {
	Convey("Test_pair_minute_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_minute:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtMinute, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 0, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(MINUTE PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtMinute)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_second(t *testing.T) {
	Convey("Test_pair_second_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_second:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtSecond, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(SECOND PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtSecond)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_datetime(t *testing.T) {
	Convey("Test_pair_datetime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_datetime:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), time.Date(2006, 1, 2, 15, 04, 04, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DATETIME PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtDatetime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_timestamp(t *testing.T) {
	Convey("Test_pair_timestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_timestamp:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtTimestamp, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999000000, time.UTC), time.Date(2006, 1, 2, 15, 04, 04, 999000000, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(TIMESTAMP PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_nanotime(t *testing.T) {
	Convey("Test_pair_nanotime_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_nanotime:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtNanoTime, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1970, 1, 1, 23, 59, 59, 999999999, time.UTC), time.Date(1970, 1, 1, 15, 4, 4, 999999999, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(NANOTIME PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTime)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_nanotimestamp(t *testing.T) {
	Convey("Test_pair_nanotimestamp_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_nanotimestamp:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtNanoTimestamp, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(NANOTIMESTAMP PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtNanoTimestamp)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_datehour(t *testing.T) {
	Convey("Test_pair_datehour_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_datehour:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtDateHour, []time.Time{time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2006, 1, 2, 15, 4, 4, 999999999, time.UTC)})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []time.Time{time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC), time.Date(2006, 1, 2, 15, 0, 0, 0, time.UTC)}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DATEHOUR PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtDateHour)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_point(t *testing.T) {
	Convey("Test_pair_point_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_point:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtPoint, [][2]float64{{-1, -1024.5}, {1001022.4, -30028.75}})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"(-1.00000, -1024.50000)", "(1001022.40000, -30028.75000)"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(POINT PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtPoint)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_complex(t *testing.T) {
	Convey("Test_pair_complex_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_complex:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtComplex, [][2]float64{{-1, -1024.5}, {1001022.4, -30028.75}})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"-1.00000+-1024.50000i", "1001022.40000+-30028.75000i"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(COMPLEX PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtComplex)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_string(t *testing.T) {
	Convey("Test_pair_string_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_string:", func() {
			data, _ := model.NewDataTypeListWithRaw(model.DtString, []string{"#$%", "数据类型"})
			pair := model.NewPair(model.NewVector(data))
			_, err := db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"#$%", "数据类型"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(STRING PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtString)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_bool(t *testing.T) {
	Convey("Test_pair_bool_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_bool:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtBool, []byte{1, 0})
			pair := model.NewPair(model.NewVector(data))
			So(err, ShouldBeNil)
			_, err = db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []bool{true, false}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(BOOL PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtBool)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_uuid(t *testing.T) {
	Convey("Test_pair_uuid_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_uuid:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtUUID, []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88"})
			pair := model.NewPair(model.NewVector(data))
			So(err, ShouldBeNil)
			_, err = db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(UUID PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtUUID)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_int128(t *testing.T) {
	Convey("Test_pair_int128_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_int128:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtInt128, []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33"})
			pair := model.NewPair(model.NewVector(data))
			So(err, ShouldBeNil)
			_, err = db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec33"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(INT128 PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtInt128)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_ipaddr(t *testing.T) {
	Convey("Test_pair_ipaddr_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_ipaddr:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtIP, []string{"0.0.0.0", "127.0.0.1"})
			pair := model.NewPair(model.NewVector(data))
			So(err, ShouldBeNil)
			_, err = db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"0.0.0.0", "127.0.0.1"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(IPADDR PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtIP)
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Pair_UpLoad_duration(t *testing.T) {
	Convey("Test_pair_duration_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_pair_duration:", func() {
			data, err := model.NewDataTypeListWithRaw(model.DtDuration, []string{"1H", "52s"})
			pair := model.NewPair(model.NewVector(data))
			So(err, ShouldBeNil)
			_, err = db.Upload(map[string]model.DataForm{"s": pair})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			re := res.(*model.Pair).Vector.Data.Value()
			zx := []string{"1H", "52s"}
			for j := 0; j < len(re); j++ {
				So(re[j], ShouldEqual, zx[j])
			}
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(DURATION PAIR)")
			So(res.GetDataType(), ShouldEqual, model.DtDuration)
		})
		So(db.Close(), ShouldBeNil)
	})
}
