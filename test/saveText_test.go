package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func CheckVectorEqual(vec *model.Vector) bool {
	data := vec.Data.Value()
	var j int32 = 1
	for i := 0; i < vec.Data.Len(); i++ {
		if j < 100 {
			if data[i] != j {
				return false
			}
		} else if j == 100 {
			j = 0
		}
		j++
	}
	return true
}
func TestSaveText(t *testing.T) {
	Convey("Test_saveText_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		WORK_DIR := setup.WORK_DIR + "/testSaveText.txt"
		Convey("Test_saveText_exception", func() {
			Convey("Test_saveText_scalar_exception", func() {
				var a string = "1"
				err = SaveText(ddb, a, WORK_DIR)
				So(err, ShouldNotBeNil)
			})
			Convey("Test_saveText_pair_exception", func() {
				var a string = "1:3"
				err = SaveText(ddb, a, WORK_DIR)
				So(err, ShouldNotBeNil)
			})
			Convey("Test_saveText_set_exception", func() {
				var a string = "set(1 2 3)"
				err = SaveText(ddb, a, WORK_DIR)
				So(err, ShouldNotBeNil)
			})
			Convey("Test_saveText_fileName_null_exception", func() {
				var a string = "table(1 2 3 as id, 4 5 6 as val)"
				err = SaveText(ddb, a, "NULL")
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_saveText_obj_vector", func() {
			var a string = "1 2 3"
			err = SaveText(ddb, a, WORK_DIR)
			So(err, ShouldBeNil)
			temp, err := LoadTextFileName(ddb, WORK_DIR)
			So(err, ShouldBeNil)
			re := temp.Data.GetColumnByName(temp.Data.GetColumnNames()[0]).String()
			So(re, ShouldEqual, "vector<int>([1, 2, 3])")
		})
		Convey("Test_saveText_obj_bigarray", func() {
			var a string = "take(1..100, 5000000)"
			err = SaveText(ddb, a, WORK_DIR)
			So(err, ShouldBeNil)
			temp, err := LoadTextFileName(ddb, WORK_DIR)
			So(err, ShouldBeNil)
			col := temp.Data.GetColumnByName(temp.Data.GetColumnNames()[0])
			re := CheckVectorEqual(col)
			So(re, ShouldBeTrue)
		})
		Convey("Test_saveText_obj_matrix", func() {
			var a string = "matrix(1 2 3, 4 5 6)"
			err = SaveText(ddb, a, WORK_DIR)
			So(err, ShouldBeNil)
			temp, err := LoadTextFileName(ddb, WORK_DIR)
			So(err, ShouldBeNil)
			re1 := temp.Data.GetColumnByName(temp.Data.GetColumnNames()[0]).String()
			So(re1, ShouldEqual, "vector<int>([1, 2, 3])")
			re2 := temp.Data.GetColumnByName(temp.Data.GetColumnNames()[1]).String()
			So(re2, ShouldEqual, "vector<int>([4, 5, 6])")
		})
		Convey("Test_saveText_obj_table", func() {
			var a string = "table(1 2 3 as id , 4 5 6 as data)"
			err = SaveText(ddb, a, WORK_DIR)
			So(err, ShouldBeNil)
			temp, err := LoadTextFileName(ddb, WORK_DIR)
			So(err, ShouldBeNil)
			re1 := temp.Data.GetColumnByName(temp.Data.GetColumnNames()[0]).String()
			So(re1, ShouldEqual, "vector<int>([1, 2, 3])")
			re2 := temp.Data.GetColumnByName(temp.Data.GetColumnNames()[1]).String()
			So(re2, ShouldEqual, "vector<int>([4, 5, 6])")
		})
	})
}
