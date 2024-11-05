package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

var host2 = getRandomClusterAddress()

func TestExistsDatabase(t *testing.T) {
	t.Parallel()
	ddb, _ := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
	Convey("Test_ExistsDatabase_ex", t, func() {
		q := api.ExistsDatabaseRequest{
			Path: "''''''''",
		}
		res, err := ddb.ExistsDatabase(&q)
		So(res, ShouldBeFalse)
		So(err.Error(), ShouldEqual, `client error response. existsDatabase(["","","","",""]) => Usage: existsDatabase(dbUrl). dbUrl must be a local path or a dfs path.`)
	})
	Convey("Test_ExistsDatabase_false", t, func() {
		q := api.ExistsDatabaseRequest{
			Path: "dfs://DLFJBNWQQQ_TEST",
		}
		res, err := ddb.ExistsDatabase(&q)
		So(res, ShouldBeFalse)
		So(err, ShouldBeNil)

	})
	Convey("Test_ExistsDatabase_true", t, func() {
		ddb.RunScript("db = database('dfs://test_existsDB', VALUE, 1..10)")
		defer ddb.RunScript("dropDatabase('dfs://test_existsDB')")
		q := api.ExistsDatabaseRequest{
			Path: "dfs://test_existsDB",
		}
		res, err := ddb.ExistsDatabase(&q)
		So(res, ShouldBeTrue)
		So(err, ShouldBeNil)
		ddb.RunScript("dropDatabase('dfs://test_existsDB')")
	})
	ddb.Close()
}

func TestExistsTable(t *testing.T) {
	t.Parallel()
	ddb, _ := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
	Convey("Test_ExistsTable_ex", t, func() {
		q := api.ExistsTableRequest{
			DBPath:    "''''''''",
			TableName: "t",
		}
		res, err := ddb.ExistsTable(&q)
		So(res, ShouldBeFalse)
		So(err.Error(), ShouldEqual, `client error response. existsTable(["","","","",""], 't') => Usage: existsTable(dbUrl, tableName). dbUrl must be a local path or a dfs path.`)
	})
	Convey("Test_ExistsTable_false", t, func() {
		q := api.ExistsTableRequest{
			DBPath:    "dfs://DLFJBNWQQQ_TEST",
			TableName: "test_existsTable",
		}
		res, err := ddb.ExistsTable(&q)
		So(res, ShouldBeFalse)
		So(err, ShouldBeNil)
	})
	Convey("Test_ExistsTable_true", t, func() {
		ddb.RunScript("db = database('dfs://test_existsTable', VALUE, 1..10); db.createTable(table(1 2 3 as c1), `test_existsTable)")
		defer ddb.RunScript("dropDatabase('dfs://test_existsTable')")
		q := api.ExistsTableRequest{
			DBPath:    "dfs://test_existsTable",
			TableName: "test_existsTable",
		}
		res, err := ddb.ExistsTable(&q)
		So(res, ShouldBeTrue)
		So(err, ShouldBeNil)
		ddb.RunScript("dropDatabase('dfs://test_existsTable')")
	})
	ddb.Close()
}

func TestCreateDatabase_ex(t *testing.T) {
	t.Parallel()
	Convey("Test_CreateDatabase_ex1", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		q := api.DatabaseRequest{
			DBHandle: "db",
		}
		db, err := ddb.Database(&q)
		So(db, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, `The function [database] expects 1~7 argument(s), but the actual number of arguments is: 0`)
		ddb.Close()
	})
}

func TestDropDatabase_ex(t *testing.T) {
	t.Parallel()
	Convey("Test_DropDataBase_ex", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		q := api.DropDatabaseRequest{
			Directory: "abcd",
		}
		err = ddb.DropDatabase(&q)
		So(err.Error(), ShouldEqual, `client error response. dropDatabase("abcd") => There is no database in the directory abcd`)
		ddb.Close()
	})
}

func TestCreateDatabase(t *testing.T) {
	t.Parallel()
	Convey("Test_CreateDatabase_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_CreateDatabase_dropDatabase", func() {
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
			}
		})
		Convey("Test_CreateDatabase_olap_value_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ddb.ExistsDatabase(&api.ExistsDatabaseRequest{Path: DfsDBPath})
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			database, err := ddb.Database(&api.DatabaseRequest{DBHandle: "db", Directory: DfsDBPath, PartitionType: "VALUE", PartitionScheme: "2010.01.01..2010.01.30"})
			So(err, ShouldBeNil)
			re2, err := ddb.ExistsDatabase(&api.ExistsDatabaseRequest{Path: DfsDBPath})
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			re3, err := ddb.RunScript("schema(db)")
			So(err, ShouldBeNil)
			re4 := re3.(*model.Dictionary)
			rePartitionType, _ := re4.Get("partitionType")
			So(rePartitionType.Value().(*model.Scalar).Value(), ShouldEqual, 1)
			reChunkGranularity, _ := re4.Get("chunkGranularity")
			So(reChunkGranularity.Value().(*model.Scalar).Value(), ShouldEqual, "TABLE")
			reAtomic, _ := re4.Get("atomic")
			So(reAtomic.Value().(*model.Scalar).Value(), ShouldEqual, "TRANS")
			rePartitionSites, _ := re4.Get("partitionSites")
			So(rePartitionSites.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
			rePartitionTypeName, _ := re4.Get("partitionTypeName")
			So(rePartitionTypeName.Value().(*model.Scalar).Value(), ShouldEqual, "VALUE")
			rePartitionSchema, _ := re4.Get("partitionSchema")
			j := 0
			for i := 1; i < 30; i++ {
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmpPartitionSchema := append([]time.Time{}, datev)
				So(rePartitionSchema.Value().(*model.Vector).Data.Value()[j], ShouldEqual, tmpPartitionSchema[0])
				j++
			}
			reDatabaseDir, _ := re4.Get("databaseDir")
			So(reDatabaseDir.Value().(*model.Scalar).Value(), ShouldEqual, DfsDBPath)
			_, err = ddb.RunScript("n=10")
			So(err, ShouldBeNil)
			_, err = ddb.Table(&api.TableRequest{TableName: "t", TableParams: []api.TableParam{
				{Key: "datev", Value: "sort(take(2010.01.01..2010.12.31, n))"},
				{Key: "int", Value: "1..n"},
				{Key: "sym", Value: `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`},
				{Key: "val", Value: "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)"},
			}})
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := database.CreatePartitionedTable(&api.CreatePartitionedTableRequest{SrcTable: "t", PartitionedTableName: DfsTBName1, PartitionColumns: []string{"datev"}})
			So(err, ShouldBeNil)
			resultDatev := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
			So(resultDatev.Data.IsNull(0), ShouldBeTrue)
			resultInt := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			So(resultInt.Data.IsNull(0), ShouldBeTrue)
			resultSym := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			So(resultSym.Data.IsNull(0), ShouldBeTrue)
			resultVal := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			So(resultVal.Data.IsNull(0), ShouldBeTrue)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + DfsTBName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			newdfstable, err := ddb.LoadTable(&api.LoadTableRequest{Database: DfsDBPath, TableName: DfsTBName1})
			So(err, ShouldBeNil)
			for i := 1; i <= 10; i++ {
				resultDatev = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
				re := resultDatev.Data.Value()
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				assert.Equal(t, re[i-1], tmp[0])
			}
			resultInt = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			re := resultInt.Data.Value()
			tmpInt := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			for i := 0; i < resultInt.Rows(); i++ {
				So(re[i], ShouldEqual, tmpInt[i])
			}
			resultSym = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			re = resultSym.Data.Value()
			tmpSym := []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(re[i], ShouldEqual, tmpSym[i])
			}
			resultVal = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			re = resultVal.Data.Value()
			tmpVal := []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				assert.Equal(t, re[i], tmpVal[i])
			}
			// create dimensionTable
			_, err = database.CreateTable(&api.CreateTableRequest{SrcTable: "t", DimensionTableName: TbName1})
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := ddb.LoadTable(&api.LoadTableRequest{Database: DfsDBPath, TableName: TbName1})
			So(err, ShouldBeNil)
			for i := 1; i <= 10; i++ {
				resultDatev = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
				re := resultDatev.Data.Value()
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				assert.Equal(t, re[i-1], tmp[0])
			}
			resultInt = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			re = resultInt.Data.Value()
			tmpInt = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			for i := 0; i < resultInt.Rows(); i++ {
				So(re[i], ShouldEqual, tmpInt[i])
			}
			resultSym = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			re = resultSym.Data.Value()
			tmpSym = []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(re[i], ShouldEqual, tmpSym[i])
			}
			resultVal = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			re = resultVal.Data.Value()
			tmpVal = []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				assert.Equal(t, re[i], tmpVal[i])
			}
			err = ddb.DropDatabase(&api.DropDatabaseRequest{Directory: DfsDBPath})
			So(err, ShouldBeNil)
			re6, err := ddb.ExistsDatabase(&api.ExistsDatabaseRequest{Path: DfsDBPath})
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
		Convey("Test_CreateDatabase_tsdb_compo_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ddb.ExistsDatabase(&api.ExistsDatabaseRequest{Path: DfsDBPath})
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			_, err = ddb.Database(&api.DatabaseRequest{DBHandle: "db1", PartitionType: "RANGE", PartitionScheme: "0 3 5 10"})
			So(err, ShouldBeNil)
			_, err = ddb.Database(&api.DatabaseRequest{DBHandle: "db2", PartitionType: "VALUE", PartitionScheme: "`AMD`QWE`CES"})
			So(err, ShouldBeNil)
			database, err := ddb.Database(&api.DatabaseRequest{DBHandle: "db", Directory: DfsDBPath, PartitionType: "COMPO", PartitionScheme: "[db1,db2]", Engine: "TSDB"})
			So(err, ShouldBeNil)
			re2, err := ddb.ExistsDatabase(&api.ExistsDatabaseRequest{Path: DfsDBPath})
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			re3, err := ddb.RunScript("schema(db)")
			So(err, ShouldBeNil)
			re4 := re3.(*model.Dictionary)
			rePartitionType, _ := re4.Get("partitionType")
			So(rePartitionType.Value().(*model.Vector).String(), ShouldEqual, "vector<int>([2, 1])")
			reChunkGranularity, _ := re4.Get("chunkGranularity")
			So(reChunkGranularity.Value().(*model.Scalar).Value(), ShouldEqual, "TABLE")
			reAtomic, _ := re4.Get("atomic")
			So(reAtomic.Value().(*model.Scalar).Value(), ShouldEqual, "TRANS")
			rePartitionSites, _ := re4.Get("partitionSites")
			So(rePartitionSites.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
			rePartitionTypeName, _ := re4.Get("partitionTypeName")
			So(rePartitionTypeName.Value().(*model.Vector).String(), ShouldEqual, "vector<string>([RANGE, VALUE])")
			rePartitionSchema, _ := re4.Get("partitionSchema")

			So(rePartitionSchema.String(), ShouldEqual, "vector<any>([vector<int>([0, 3, 5, 10]), vector<symbol>([AMD, CES, QWE])])")
			reDatabaseDir, _ := re4.Get("databaseDir")
			So(reDatabaseDir.Value().(*model.Scalar).Value(), ShouldEqual, DfsDBPath)
			_, err = ddb.RunScript("n=10")
			So(err, ShouldBeNil)
			_, err = ddb.Table(&api.TableRequest{TableName: "t", TableParams: []api.TableParam{
				{Key: "datev", Value: "sort(take(2010.01.01..2010.01.31, n))"},
				{Key: "id", Value: "1..n"},
				{Key: "sym", Value: `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`},
				{Key: "val", Value: "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)"},
			}})
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := database.CreatePartitionedTable(&api.CreatePartitionedTableRequest{SrcTable: "t", PartitionedTableName: DfsTBName1, PartitionColumns: []string{"id", "sym"}, SortColumns: []string{"datev"}})
			So(err, ShouldBeNil)
			resultDatev := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
			So(resultDatev.Data.IsNull(0), ShouldBeTrue)
			resultInt := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			So(resultInt.Data.IsNull(0), ShouldBeTrue)
			resultSym := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			So(resultSym.Data.IsNull(0), ShouldBeTrue)
			resultVal := dfsTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			So(resultVal.Data.IsNull(0), ShouldBeTrue)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + DfsTBName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			newdfstable, err := ddb.LoadTable(&api.LoadTableRequest{Database: DfsDBPath, TableName: DfsTBName1})
			So(err, ShouldBeNil)
			tmp, _ := ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + DfsTBName1 + `")`)
			for i := 0; i < newdfstable.Data.Columns(); i++ {
				res := newdfstable.Data.GetColumnByIndex(i).Data.Value()
				ex := tmp.(*model.Table).GetColumnByIndex(i).Data.Value()
				So(res, ShouldResemble, ex)
			}
			resultInt = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			re := resultInt.Data.Value()
			tmpInt := []int32{1, 2, 3, 4, 5, 8, 7, 9, 6}
			for i := 0; i < resultInt.Rows(); i++ {
				So(re[i], ShouldEqual, tmpInt[i])
			}
			resultSym = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			re = resultSym.Data.Value()

			tmpSym := []string{"AMD", "QWE", "CES", "DOP", "ASZ", "AWQ", "BBVC", "DS", "FSD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(re[i], ShouldEqual, tmpSym[i])
			}
			resultVal = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			re = resultVal.Data.Value()
			tmpVal := []int32{39, 50, 5, 24, 79, 67, 8, 29, 39}
			for i := 0; i < resultVal.Rows(); i++ {
				So(re[i], ShouldEqual, tmpVal[i])
			}
			// create dimensionTable
			_, err = database.CreateTable(&api.CreateTableRequest{SrcTable: "t", DimensionTableName: TbName1, SortColumns: []string{"datev"}})
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := ddb.LoadTable(&api.LoadTableRequest{Database: DfsDBPath, TableName: TbName1})
			So(err, ShouldBeNil)
			for i := 1; i <= 10; i++ {
				resultDatev = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
				re := resultDatev.Data.Value()
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				assert.Equal(t, re[i-1], tmp[0])
			}
			resultInt = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			re = resultInt.Data.Value()

			tmpInt = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			for i := 0; i < resultInt.Rows(); i++ {
				So(re[i], ShouldEqual, tmpInt[i])
			}
			resultSym = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			re = resultSym.Data.Value()
			fmt.Println(re)
			tmpSym = []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(re[i], ShouldEqual, tmpSym[i])
			}
			resultVal = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			re = resultVal.Data.Value()
			tmpVal = []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				assert.Equal(t, re[i], tmpVal[i])
			}
			err = ddb.DropDatabase(&api.DropDatabaseRequest{Directory: DfsDBPath})
			So(err, ShouldBeNil)
			re6, err := ddb.ExistsDatabase(&api.ExistsDatabaseRequest{Path: DfsDBPath})
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
	})
}

func TestDataBaseFunctions(t *testing.T) {
	t.Parallel()
	Convey("Test_CreateDatabase_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("TestCreateDatabase_dropDatabase", func() {
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
			}
		})
		Convey("Test_getSession", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			db, err := ddb.Database(&api.DatabaseRequest{DBHandle: "db", Directory: DfsDBPath, PartitionType: "RANGE", PartitionScheme: "0 3 5 10"})
			So(err, ShouldBeNil)
			sname := db.GetSession()
			fmt.Println(sname)
			ex_sname, _ := ddb.RunScript("getCurrentSessionAndUser()[0]")
			fmt.Println(ex_sname)
			So("long("+sname+")", ShouldEqual, ex_sname.(*model.Scalar).String())
			err = ddb.DropDatabase(&api.DropDatabaseRequest{Directory: DfsDBPath})
			So(err, ShouldBeNil)
		})
		ddb.Close()
	})
}
