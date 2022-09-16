package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func checkVectorisNull(arr *model.Vector) bool {
	for i := 0; i < arr.Rows(); i++ {
		re := arr.Data.IsNull(i)
		if re != true {
			return false
		}
	}
	return true
}

func TestRunScript(t *testing.T) {
	Convey("test_RunScript_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("test_RunScript_func", func() {
			Convey("test_RunScript_bool_scalar", func() {
				tmp, err := ddb.RunScript("true")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "bool")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				So(re, ShouldEqual, true)
				tmp, err = ddb.RunScript("bool()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_char_scalar", func() {
				tmp, err := ddb.RunScript("'a'")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "char")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				var ex byte = 97
				So(re, ShouldEqual, ex)
				tmp, err = ddb.RunScript("char()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_short_scalar", func() {
				tmp, err := ddb.RunScript("22h")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "short")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				var ex int16 = 22
				So(re, ShouldEqual, ex)
				tmp, err = ddb.RunScript("short()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_int_scalar", func() {
				tmp, err := ddb.RunScript("22")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "int")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				var ex int32 = 22
				So(re, ShouldEqual, ex)
				tmp, err = ddb.RunScript("int()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_long_vector", func() {
				tmp, err := ddb.RunScript("22l 200l")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "long")
				result := tmp.(*model.Vector)
				re := result.Data.Value()
				var ex1 int64 = 22
				var ex2 int64 = 200
				So(re[0], ShouldEqual, ex1)
				So(re[1], ShouldEqual, ex2)
				tmp, err = ddb.RunScript("take(00i, 10)")
				So(err, ShouldBeNil)
				result = tmp.(*model.Vector)
				rs := checkVectorisNull(result)
				So(rs, ShouldEqual, true)
			})
		})
	})
}
