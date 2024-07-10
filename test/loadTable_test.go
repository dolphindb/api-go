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

var host9 = getRandomClusterAddress()

func TestLoadTable(t *testing.T) {
	t.Parallel()
	Convey("Test LoadTable prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host9, setup.UserName, setup.Password)
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
		Convey("Test_LoadTable_dfs_dimension:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsDimensiondb(DfsDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(fmt.Sprintf(`select * from %s`, "''"))
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, `RefId:S02033`)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			re2 := CompareTablesDataformTable(exTmp, reTmp)
			So(re2, ShouldBeTrue)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_range:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsRangedb(DfsDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			re2 := CompareTablesDataformTable(exTmp, reTmp)
			So(re2, ShouldBeTrue)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_range_memoryMode_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsRangedb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTableMemoryMode(ddb, TbName1, DfsDBPath, true)
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_range_partitions_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsRangedb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTablePartitions(ddb, TbName1, DfsDBPath, "5000")
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_hash:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsHashdb(DfsDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			re2 := CompareTablesDataformTable(exTmp, reTmp)
			So(re2, ShouldBeTrue)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_hash_memoryMode_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsHashdb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTableMemoryMode(ddb, TbName1, DfsDBPath, true)
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_hash_partitions_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsHashdb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTablePartitions(ddb, TbName1, DfsDBPath, "1")
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_value:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsValuedb(DfsDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			re2 := CompareTablesDataformTable(exTmp, reTmp)
			So(re2, ShouldBeTrue)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_value_memoryMode_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsValuedb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTableMemoryMode(ddb, TbName1, DfsDBPath, true)
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_value_partitions_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsValuedb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTablePartitions(ddb, TbName1, DfsDBPath, "2010.01.01")
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_list:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsListdb(DfsDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			re2 := CompareTablesDataformTable(exTmp, reTmp)
			So(re2, ShouldBeTrue)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_list_memoryMode_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsListdb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTableMemoryMode(ddb, TbName1, DfsDBPath, true)
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_list_partitions_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsListdb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTablePartitions(ddb, TbName1, DfsDBPath, "`DOP")
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_compo:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeRangedb(DfsDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			re2 := CompareTablesDataformTable(exTmp, reTmp)
			So(re2, ShouldBeTrue)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re3, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re3, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_compo_range_range_memoryMode_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeRangedb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTableMemoryMode(ddb, TbName1, DfsDBPath, true)
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_dfs_compo_range_range_partitions_exception:", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			CreateDfsCompoRangeRangedb(DfsDBPath, TbName1, TbName2)
			_, err = LoadTablePartitions(ddb, TbName1, DfsDBPath, "2010.01.01")
			So(err, ShouldNotBeNil)
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeFalse)
		})
		Convey("Test_LoadTable_disk_unpartitioned:", func() {
			CreateDiskUnpartitioneddb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_range:", func() {
			CreateDiskRangedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_range_partition:", func() {
			CreateDiskRangedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `") where id < 20001`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTablePartitions(ddb, TbName1, DiskDBPath, `[5000, 15000]`)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_range_memoryMode:", func() {
			CreateDiskRangedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			// before, _ := ddb.RunScript("exec memSize from getSessionMemoryStat()")
			reTmp, err := LoadTableMemoryMode(ddb, TbName1, DiskDBPath, true)
			So(err, ShouldBeNil)
			// after, _ := ddb.RunScript("exec memSize from getSessionMemoryStat()")
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
			// before1 := before.(*model.Vector).Data.Value()[1]
			// after1 := after.(*model.Vector).Data.Value()[1]
			// So(after1, ShouldBeGreaterThanOrEqualTo, before1)
		})
		Convey("Test_LoadTable_disk_hash:", func() {
			CreateDiskHashdb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_hash_partition:", func() {
			CreateDiskHashdb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `") where id in [1, 3, 5]`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTablePartitions(ddb, TbName1, DiskDBPath, "[1, 3, 5]")
			So(err, ShouldBeNil)
			re := CompareTablesDataformTable(exTmp, reTmp)
			So(re, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_hash_memoryMode:", func() {
			CreateDiskHashdb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			before, _ := ddb.RunScript("exec memSize from getSessionMemoryStat()")
			reTmp, err := LoadTableMemoryMode(ddb, TbName1, DiskDBPath, true)
			So(err, ShouldBeNil)
			after, _ := ddb.RunScript("exec memSize from getSessionMemoryStat()")
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
			before1 := before.(*model.Vector).Data.Value()[1]
			after1 := after.(*model.Vector).Data.Value()[1]
			So(after1, ShouldBeGreaterThanOrEqualTo, before1)
		})
		Convey("Test_LoadTable_disk_value:", func() {
			CreateDiskValuedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_value_partition:", func() {
			CreateDiskValuedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `") where id in [2010.01.01, 2010.01.30]`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTablePartitions(ddb, TbName1, DiskDBPath, "[2010.01.01, 2010.01.30]")
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_list:", func() {
			CreateDiskListdb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_list_partition:", func() {
			CreateDiskListdb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `") where sym in ["DOP", "ASZ", "FSD", "BBVC","AWQ","DS"]`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTablePartitions(ddb, TbName1, DiskDBPath, `["DOP", "FSD", "AWQ"]`)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_list_memoryMode:", func() {
			CreateDiskValuedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			before, _ := ddb.RunScript("exec memSize from getSessionMemoryStat()")
			reTmp, err := LoadTableMemoryMode(ddb, TbName1, DiskDBPath, true)
			So(err, ShouldBeNil)
			after, _ := ddb.RunScript("exec memSize from getSessionMemoryStat()")
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
			before1 := before.(*model.Vector).Data.Value()[1]
			after1 := after.(*model.Vector).Data.Value()[1]
			So(after1, ShouldBeGreaterThanOrEqualTo, before1)
		})
		Convey("Test_LoadTable_disk_compo_range_range:", func() {
			CreateDiskCompoRangeRangedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `")`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTable(ddb, TbName1, DiskDBPath)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_disk_compo_range_range_partition:", func() {
			CreateDiskCompoRangeRangedb(DiskDBPath, TbName1, TbName2)
			tmp, err := ddb.RunScript(`select * from loadTable("` + DiskDBPath + `", "` + TbName1 + `") where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30`)
			So(err, ShouldBeNil)
			exTmp := tmp.(*model.Table)
			reTmp, err := LoadTablePartitions(ddb, TbName1, DiskDBPath, `[2010.01.01, 2010.04.25]`)
			So(err, ShouldBeNil)
			re1 := CompareTablesDataformTable(exTmp, reTmp)
			So(re1, ShouldBeTrue)
		})
		Convey("Test_LoadTable_more_than_once", func() {
			dbName := "dfs://test_" + generateRandomString(5)
			tbName := "test_table"
			_, err := ddb.RunScript(`db = database("` + dbName + `", VALUE, 1..5);
						   t = table(1 2 3 as c1, rand(100.00, 3) as c2);
						   db.createPartitionedTable(t, '` + tbName + `', 'c1').append!(t)`)
			So(err, ShouldBeNil)
			l := &api.LoadTableRequest{
				Database:  dbName,
				TableName: tbName,
			}
			for i := 0; i < 1000; i++ {
				lt, err := ddb.LoadTable(l)
				So(err, ShouldBeNil)
				s := lt.GetHandle()
				res, _ := ddb.RunScript("select * from " + s)
				ex, _ := ddb.RunScript(s + `=select * from loadTable("` + dbName + `", "` + tbName + `");` + s)
				So(res, ShouldEqual, ex.(*model.Table))
			}
		})
	})
}
