package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDropPartition_tabletMultiple(t *testing.T) {
	Convey("Test_DropPartition_tabletMultiple_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Drop all Databases", func() {
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
		Convey("Test_DropPartition_tabletMultiple_range_drop_single:", func() {
			Convey("Test_DropPartition_tabletMultiple_range_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "10001", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/1_10001'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_range_drop_all_tables:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "10001", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/1_10001'")
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, "'/1_10001'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_range_drop_multiple:", func() {
			Convey("Test_DropPartition_tabletMultiple_range_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "30001", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/1_10001', '/10001_20001', '/20001_30001']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_range_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "30001", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/1_10001', '/10001_20001', '/20001_30001']`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `['/1_10001', '/10001_20001', '/20001_30001']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_hash_drop_single:", func() {
			Convey("Test_DropPartition_tabletMultiple_hash_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsHashdb(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "10", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id != %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/Key0'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_hash_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsHashdb(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "10", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id != %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/Key0'")
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, "'/Key0'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_hash_drop_multiple:", func() {
			Convey("Test_DropPartition_tabletMultiple_hash_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsHashdb(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2..9", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id in %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/Key0', '/Key1']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_hash_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsHashdb(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2..9", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where id in %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/Key0', '/Key1']`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `['/Key0', '/Key1']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_value_drop_single:", func() {
			Convey("Test_DropPartition_tabletMultiple_value_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsValuedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.01.01", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date != %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/20100101'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_value_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsValuedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.01.01", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date != %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/20100101'")
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, "'/20100101'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_value_drop_multiple:", func() {
			Convey("Test_DropPartition_tabletMultiple_value_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsValuedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "!in(date, 2010.01.01+[0, 7, 14, 21])", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/20100101', '/20100108', '/20100115', '/20100122']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_value_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsValuedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "!in(date, 2010.01.01+[0, 7, 14, 21])", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/20100101', '/20100108', '/20100115', '/20100122']`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `['/20100101', '/20100108', '/20100115', '/20100122']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_list_drop_single:", func() {
			Convey("Test_DropPartition_tabletMultiple_list_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsListdbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "!in(sym,`AMD`QWE`CES)", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/List0'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_list_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsListdbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "!in(sym,`AMD`QWE`CES)", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "'/List0'")
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, "'/List0'")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_list_drop_multiple:", func() {
			Convey("Test_DropPartition_tabletMultiple_list_drop_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsListdbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "!in(sym,`DOP`ASZ`FSD`BBVC)", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/List1', '/List2']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_list_drop_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsListdbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "!in(sym,`DOP`ASZ`FSD`BBVC)", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `['/List1', '/List2']`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `['/List1', '/List2']`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level1_single:", func() {
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level1_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.02.01", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "2010.01.01")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level1_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.02.01", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, "2010.01.01")
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, "2010.01.01")
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level1_multiple:", func() {
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level1_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.03.01", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `[2010.01.01, 2010.02.01]`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level1_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.03.01", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `[2010.01.01, 2010.02.01]`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `[2010.01.01, 2010.02.01]`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level2_single:", func() {
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level2_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.02.01 or id >= 3", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `[2010.01.01, 1]`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level2_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "2010.02.01 or id >= 3", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `[2010.01.01, 1]`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `[2010.01.01, 1]`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
		Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level2_multiple:", func() {
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level2_only_one_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "date >= 2010.03.01 or !between(id, 3:6)", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `[[2010.01.01,2010.02.01], [3,5]]`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, originTable2)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
			Convey("Test_DropPartition_tabletMultiple_compo_range_range_drop_level2_all_table:", func() {
				re1, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re1, ShouldBeFalse)
				CreateDfsCompoRangeRangedbChunkGranularity(DfsDBPath, DfsTBName1, DfsTBName2)
				re2, err := ExistsDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
				So(re2, ShouldBeTrue)
				originTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				originTable2, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				re := CompareTables(originTable1, originTable2)
				So(err, ShouldBeNil)
				So(re, ShouldBeTrue)
				rs, err := LoadTableBySQL(ddb, "date >= 2010.03.01 or !between(id, 3:6)", "select * from loadTable('"+DfsDBPath+"','"+DfsTBName1+"') where date >= %s", DfsDBPath, DfsTBName1)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName1, DfsDBPath, `[[2010.01.01,2010.02.01], [3,5]]`)
				So(err, ShouldBeNil)
				err = DropPartition(ddb, DfsTBName2, DfsDBPath, `[[2010.01.01,2010.02.01], [3,5]]`)
				So(err, ShouldBeNil)
				reTable1, err := LoadTable(ddb, DfsTBName1, DfsDBPath)
				So(err, ShouldBeNil)
				reTable2, err := LoadTable(ddb, DfsTBName2, DfsDBPath)
				So(err, ShouldBeNil)
				reData1 := CompareTables(reTable1, rs)
				So(reData1, ShouldBeTrue)
				reData2 := CompareTables(reTable2, rs)
				So(reData2, ShouldBeTrue)
				err = DropDatabase(ddb, DfsDBPath)
				So(err, ShouldBeNil)
			})
		})
	})
}
