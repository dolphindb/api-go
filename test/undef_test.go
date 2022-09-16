package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func CompareTablesTwoDataformTable(tableName1 *model.Table, tableName2 *model.Table) bool {
	if tableName1.Columns() == tableName2.Columns() && tableName1.GetDataTypeString() == tableName2.GetDataTypeString() && tableName1.GetDataForm() == tableName2.GetDataForm() {
		for i := 0; i < tableName1.Columns(); i++ {
			reTable1 := tableName1.GetColumnByName(tableName1.GetColumnNames()[i]).Data.Value()
			reTable2 := tableName2.GetColumnByName(tableName2.GetColumnNames()[i]).Data.Value()
			for i := 0; i < tableName1.Rows(); i++ {
				if reTable1[i] == reTable2[i] {
					continue
				} else {
					return false
				}
			}
		}
		return true
	}
	return false
}
func TestUndef(t *testing.T) {
	Convey("Test_func_undef_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_func_undef_varible_data", func() {
			_, err = ddb.RunScript("x=1")
			So(err, ShouldBeNil)
			undefReq := new(api.UndefRequest).
				SetObj("`x").SetObjType("VAR")
			err = ddb.Undef(undefReq)
			So(err, ShouldBeNil)
			_, err := ddb.RunScript("x")
			So(err, ShouldNotBeNil)
		})
		Convey("Test_func_undef_only_SetObj", func() {
			_, err = ddb.RunScript("x=1;")
			So(err, ShouldBeNil)
			undefReq := new(api.UndefRequest).
				SetObj("`x")
			err = ddb.Undef(undefReq)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript("x")
			So(err, ShouldNotBeNil)
		})
		Convey("Test_func_undef_varible_data_SetObj", func() {
			_, err = ddb.RunScript("def f(a){return a+1}")
			So(err, ShouldBeNil)
			undefReq := new(api.UndefRequest).
				SetObj("`f").SetObjType("DEF")
			err = ddb.Undef(undefReq)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript("f")
			So(err, ShouldNotBeNil)
		})

		Convey("Test_func_undef_varible_data_list", func() {
			_, err = ddb.RunScript("x=1;y=short(10)")
			So(err, ShouldBeNil)
			undefReq := new(api.UndefRequest).
				SetObj("`x`y")
			err = ddb.Undef(undefReq)
			So(err, ShouldBeNil)
			res, err := ddb.RunScript("x")
			fmt.Println(res)
			So(err, ShouldNotBeNil)
			_, err = ddb.RunScript("y")
			So(err, ShouldNotBeNil)
		})
	})
}

func TestUndefAll(t *testing.T) {
	Convey("Test_func_UndefAll_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_func_UndefAll_varible_data", func() {
			_, err = ddb.RunScript("x=1")
			So(err, ShouldBeNil)
			err = ddb.UndefAll()
			So(err, ShouldBeNil)
			_, err := ddb.RunScript("x")
			So(err, ShouldNotBeNil)
		})
		Convey("Test_func_undef_table", func() {
			_, err = ddb.RunScript("t=table(1..10 as id)")
			So(err, ShouldBeNil)
			err = ddb.UndefAll()
			So(err, ShouldBeNil)
			_, err = ddb.RunScript("t")
			So(err, ShouldNotBeNil)
		})
	})
}

func TestClearAllCache(t *testing.T) {
	Convey("Test_func_ClearAllCache_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_func_ClearAllCache_SetIsDFS_true", func() {
			_, err := ddb.RunScript(
				`dbPath = "dfs://PTA_test"
			if(existsDatabase(dbPath))
					dropDatabase(dbPath)
			t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
			db=database(dbPath, RANGE, symbol("A"+string(1..6)))
			pt = db.createPartitionedTable(t, "pt", "sym")
			t=table(["A1", "A2", "A3", "A4", "A5"] as sym, 2 7 12 22 24 as id, 1970.01.01 1969.12.02 1970.03.01 1969.10.02 1970.05.01 as datev, 21.2 4.4 5.5 2.3 6.6 as price)
			pt.append!(t)`)
			So(err, ShouldBeNil)
			orginaltable, err := ddb.RunScript("select * from loadTable(dbPath, \"pt\")")
			So(err, ShouldBeNil)
			c := new(api.ClearAllCacheRequest).
				SetIsDFS(true)
			err = ddb.ClearAllCache(c)
			So(err, ShouldBeNil)
			restable, err := ddb.RunScript("select * from loadTable(dbPath, \"pt\")")
			So(err, ShouldBeNil)
			re := CompareTablesTwoDataformTable(orginaltable.(*model.Table), restable.(*model.Table))
			So(re, ShouldBeTrue)
		})

		Convey("Test_func_ClearAllCache_SetIsDFS_false", func() {
			_, err := ddb.RunScript(
				`dbPath = "dfs://PTA_test"
			if(existsDatabase(dbPath))
					dropDatabase(dbPath)
			t = table(100:100, ["sym", "id", "datev", "price"],[SYMBOL, INT, DATE, DOUBLE])
			db=database(dbPath, RANGE, symbol("A"+string(1..6)))
			pt = db.createPartitionedTable(t, "pt", "sym")
			t=table(["A1", "A2", "A3", "A4", "A5"] as sym, 2 7 12 22 24 as id, 1970.01.01 1969.12.02 1970.03.01 1969.10.02 1970.05.01 as datev, 21.2 4.4 5.5 2.3 6.6 as price)
			pt.append!(t)`)
			So(err, ShouldBeNil)
			orginaltable, err := ddb.RunScript("select * from loadTable(dbPath, \"pt\")")
			So(err, ShouldBeNil)
			c := new(api.ClearAllCacheRequest).
				SetIsDFS(false)
			err = ddb.ClearAllCache(c)
			So(err, ShouldBeNil)
			restable, err := ddb.RunScript("select * from loadTable(dbPath, \"pt\")")
			So(err, ShouldBeNil)
			re := CompareTablesTwoDataformTable(orginaltable.(*model.Table), restable.(*model.Table))
			So(re, ShouldBeTrue)
		})
	})
}
