package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var host17 = getRandomClusterAddress()

func TestSaveTable(t *testing.T) {
	t.Parallel()
	Convey("Test_function_SaveTable_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host17, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Drop all Databases", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			dbPaths := []string{DfsDBPath, DiskDBPath}
			for _, dbPath := range dbPaths {
				script := `
				if(existsDatabase("` + dbPath + `")){
						dropDatabase("` + dbPath + `")
				}
				if(exists("` + dbPath + `")){
					rmdir("` + dbPath + `", true)
				}
				`
				_, err = ddb.RunScript(script)
				So(err, ShouldBeNil)
				re, err := ddb.RunScript(`existsDatabase("` + dbPath + `")`)
				So(err, ShouldBeNil)
				isExitsDatabase := re.(*model.Scalar).DataType.Value()
				So(isExitsDatabase, ShouldBeFalse)
			}
		})
		_, err = ddb.RunScript(`t=table(1..10 as id, 1969.12.26+ 1..10 as datev, "A"+string(1..10) as str)`)
		So(err, ShouldBeNil)
		Convey("Test_function_SaveTable_DBHandle_exception", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath("dsa")
			err = ddb.SaveTable(l)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_function_SaveTable_disk_unpartitioned", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t")
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, "t", DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetTableName", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetAppending_true", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(true)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
			l1 := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(true)
			err = ddb.SaveTable(l1)
			So(err, ShouldBeNil)
			reTmp1, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID1 := reTmp1.Data.GetColumnByName("id")
			redatev1 := reTmp1.Data.GetColumnByName("datev")
			restr1 := reTmp1.Data.GetColumnByName("str")
			So(reID1.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev1.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05, 1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr1.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetAppending_false", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(false)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetAppending_false_SetCompression_true", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(false).SetCompression(true)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetAppending_false_SetCompression_false", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(false).SetCompression(false)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetAppending_true_SetCompression_false", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`t=table(1..10 as id, 1969.12.26+ 1..10 as datev, "A"+string(1..10) as str)`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(true).SetCompression(false)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_SetAppending_true_SetCompression_true", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetAppending(true).SetCompression(true)
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_SaveTable_dbhandler", func() {
			_, err = ddb.RunScript(`if(exists("` + DiskDBPath + `")){
				rmdir("` + DiskDBPath + `", true)}`)
			So(err, ShouldBeNil)
			_, err := ddb.RunScript(`db=database("` + DiskDBPath + `")`)
			So(err, ShouldBeNil)
			l := new(api.SaveTableRequest).
				SetDBPath(DiskDBPath).SetTable("t").SetTableName(MemTableName).SetDBHandle("db")
			err = ddb.SaveTable(l)
			So(err, ShouldBeNil)
			reTmp, err := LoadTable(ddb, MemTableName, DiskDBPath)
			So(err, ShouldBeNil)
			reID := reTmp.Data.GetColumnByName("id")
			redatev := reTmp.Data.GetColumnByName("datev")
			restr := reTmp.Data.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(redatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(restr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
	})
}
