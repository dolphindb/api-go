package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDropTableException(t *testing.T) {
	Convey("Test_existsTable_and_dropTable_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_existsTable_dropDatabase", func() {
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
		Convey("Test_dropTable_wrong_Table_exception", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, "mt1")
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsDimensiondb(DfsDBPath, TbName1, TbName2)
			err = DropTable(ddb, "mt", DfsDBPath)
			So(err, ShouldNotBeNil)
		})
		Convey("Test_dropTable_wrong_dbpath_exception", func() {
			re1, err := ExistsTable(ddb, "dfs://test1", TbName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsDimensiondb(DfsDBPath, TbName1, TbName2)
			err = DropTable(ddb, TbName1, "dfs://test1")
			So(err, ShouldNotBeNil)
		})
		Convey("Test_dropTable_only_DBHandle_dbPath_exception", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, TbName1)
			So(err, ShouldBeNil)
			if re1 == true {
				_, err = ddb.RunScript("dropDatabase('" + DfsDBPath + "')")
				So(err, ShouldBeNil)
			}
			ddbScript := `
			dbPath="` + DfsDBPath + `"
			if(existsDatabase(dbPath))
					dropDatabase(dbPath)
			db=database(dbPath, RANGE, 1..10)
			n=100000
			tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
			db.createTable(tdata, "` + TbName1 + `").append!(tdata)
			db.createTable(tdata, "` + TbName2 + `").append!(tdata)
			`
			_, err = ddb.RunScript(ddbScript)
			So(err, ShouldBeNil)
			t := new(api.DropTableRequest).
				SetDBPath(DfsDBPath).SetDBHandle("db")
			err = ddb.DropTable(t)
			So(err, ShouldNotBeNil)
		})
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}

func TestExistsTableAndDropTable(t *testing.T) {
	Convey("Test_existsTable_and_dropTable_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_existsTable_dropDatabase", func() {
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
		Convey("Test_existsTable_dfs_dimension", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, TbName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			re2, err := ExistsTable(ddb, DfsDBPath, TbName2)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
			CreateDfsDimensiondb(DfsDBPath, TbName1, TbName2)
			re3, err := ExistsTable(ddb, DfsDBPath, TbName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeTrue)
			re4, err := ExistsTable(ddb, DfsDBPath, TbName2)
			So(err, ShouldBeNil)
			So(re4, ShouldBeTrue)
			err = DropTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			err = DropTable(ddb, TbName2, DfsDBPath)
			So(err, ShouldBeNil)
			re5, err := ExistsTable(ddb, DfsDBPath, TbName1)
			So(err, ShouldBeNil)
			So(re5, ShouldBeFalse)
			re6, err := ExistsTable(ddb, DfsDBPath, TbName2)
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_value", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsValuedb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_range", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsRangedb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_hash", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsHashdb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_list", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsListdb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_compo_range_range", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeRangedb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_compo_range_value", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeValuedb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_compo_range_list", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeListdb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_dfs_compo_range_hash", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeHashdb(DfsDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_disk_unpartitioned_table", func() {
			re1, err := ExistsTable(ddb, DfsDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDiskUnpartitioneddb(DiskDBPath, TbName1, TbName2)
			re2, err := ExistsTable(ddb, DiskDBPath, TbName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DiskDBPath, "tdata")
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
			err = DropDatabase(ddb, DiskDBPath)
			So(err, ShouldBeNil)
		})
		Convey("Test_existsTable_disk_range", func() {
			re1, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDiskRangedb(DiskDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DiskDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_disk_value", func() {
			re1, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDiskValuedb(DiskDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DiskDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_disk_list", func() {
			re1, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDiskListdb(DiskDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DiskDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_disk_hash", func() {
			re1, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDiskHashdb(DiskDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DiskDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_existsTable_disk_compo_range_range", func() {
			re1, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDiskCompoRangeRangedb(DiskDBPath, DfsTBName1, DfsTBName2)
			re2, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			err = DropTable(ddb, DfsTBName1, DiskDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsTable(ddb, DiskDBPath, DfsTBName1)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
	})
}
