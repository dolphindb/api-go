package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

var host2 = getRandomClusterAddress()
func TestExistsDatabase_ex(t *testing.T) {
	t.Parallel()
	Convey("Test_ExistsDatabase_ex", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = ExistsDatabase(ddb, "''''''''")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `client error response. existsDatabase(["","","","",""]) => Usage: existsDatabase(dbUrl). dbUrl must be a local path or a dfs path.`)
		ddb.Close()
	})
}

func TestExistsTable_ex(t *testing.T) {
	t.Parallel()
	Convey("Test_TestExistsTable_ex", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = ddb.RunScript(fmt.Sprintf("existsTable('%s','%s')", "''", "tables"))
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `client error response. existsTable(["",""], "tables") => Usage: existsTable(dbUrl, tableName). dbUrl must be a local path or a dfs path.`)
		ddb.Close()
	})
}

func TestCreateDatabase_ex(t *testing.T) {
	t.Parallel()
	Convey("Test_CreateDatabase_ex", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = ddb.RunScript(fmt.Sprintf("%s=database(%s)", DBhandler, "db"))
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `client error response. db = database(db) => Variable 'db' isn't initialized yet.`)
		ddb.Close()
	})
}

func TestDropDatabase_ex(t *testing.T) {
	t.Parallel()
	Convey("Test_DropDataBase_ex", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host2, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = ddb.RunScript(fmt.Sprintf("dropDatabase('%s')", "nj"))
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, `client error response. dropDatabase("nj") => There is no database in the directory nj`)
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
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			database, err := CreateDatabase(ddb, DfsDBPath, DBhandler, "VALUE", "2010.01.01..2010.01.30", "", "", "")
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
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
			_, err = CreateMemTable(ddb, "t", "datev", "id", "sym", "val", "sort(take(2010.01.01..2010.12.31, n))", "1..n", `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`, "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)")
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := CreateDefPartitionedTable(database, "t", DfsTBName1, []string{"datev"})
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
			newdfstable, err := LoadPartitionedTable(ddb, DfsTBName1, DfsDBPath)
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
			_, err = CreateTable(database, "t", TbName1)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := LoadTable(ddb, TbName1, DfsDBPath)
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
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re6, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
		Convey("Test_CreateDatabase_olap_range_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			database, err := CreateDatabase(ddb, DfsDBPath, DBhandler, "RANGE", "0 3 5 10", "", "", "")
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			re3, err := ddb.RunScript("schema(db)")
			So(err, ShouldBeNil)
			re4 := re3.(*model.Dictionary)
			rePartitionType, _ := re4.Get("partitionType")
			So(rePartitionType.Value().(*model.Scalar).Value(), ShouldEqual, 2)
			reChunkGranularity, _ := re4.Get("chunkGranularity")
			So(reChunkGranularity.Value().(*model.Scalar).Value(), ShouldEqual, "TABLE")
			reAtomic, _ := re4.Get("atomic")
			So(reAtomic.Value().(*model.Scalar).Value(), ShouldEqual, "TRANS")
			rePartitionSites, _ := re4.Get("partitionSites")
			So(rePartitionSites.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
			rePartitionTypeName, _ := re4.Get("partitionTypeName")
			So(rePartitionTypeName.Value().(*model.Scalar).Value(), ShouldEqual, "RANGE")
			rePartitionSchema, _ := re4.Get("partitionSchema")
			tmpPartitionSchema := []int{0, 3, 5, 10}
			for i := 0; i < len(rePartitionSchema.Value().(*model.Vector).Data.Value()); i++ {
				So(rePartitionSchema.Value().(*model.Vector).Data.Value()[i], ShouldEqual, tmpPartitionSchema[i])
			}
			reDatabaseDir, _ := re4.Get("databaseDir")
			So(reDatabaseDir.Value().(*model.Scalar).Value(), ShouldEqual, DfsDBPath)
			_, err = ddb.RunScript("n=10")
			So(err, ShouldBeNil)
			_, err = CreateMemTable(ddb, "t", "datev", "id", "sym", "val", "take(2010.01.01..2010.01.31, n)", "[1,4,5,5,6,6,6,6,8,8]", `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`, "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)")
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := CreateDefPartitionedTable(database, "t", DfsTBName1, []string{"id"})
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
			newdfstable, err := LoadPartitionedTable(ddb, DfsTBName1, DfsDBPath)
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
			tmpInt := []int32{1, 4, 5, 5, 6, 6, 6, 6, 8, 8}
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
			_, err = CreateTable(database, "t", TbName1)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := LoadTable(ddb, TbName1, DfsDBPath)
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
			tmpInt = []int32{1, 4, 5, 5, 6, 6, 6, 6, 8, 8}
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
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re6, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
		Convey("Test_CreateDatabase_olap_hash_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			database, err := CreateDatabase(ddb, DfsDBPath, DBhandler, "HASH", "[INT, 3]", "", "", "")
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			re3, err := ddb.RunScript("schema(db)")
			So(err, ShouldBeNil)
			re4 := re3.(*model.Dictionary)
			rePartitionType, _ := re4.Get("partitionType")
			So(rePartitionType.Value().(*model.Scalar).Value(), ShouldEqual, 5)
			reChunkGranularity, _ := re4.Get("chunkGranularity")
			So(reChunkGranularity.Value().(*model.Scalar).Value(), ShouldEqual, "TABLE")
			reAtomic, _ := re4.Get("atomic")
			So(reAtomic.Value().(*model.Scalar).Value(), ShouldEqual, "TRANS")
			rePartitionSites, _ := re4.Get("partitionSites")
			So(rePartitionSites.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
			rePartitionTypeName, _ := re4.Get("partitionTypeName")
			So(rePartitionTypeName.Value().(*model.Scalar).Value(), ShouldEqual, "HASH")
			rePartitionSchema, _ := re4.Get("partitionSchema")
			So(rePartitionSchema.Value().(*model.Scalar).Value(), ShouldEqual, 3)
			reDatabaseDir, _ := re4.Get("databaseDir")
			So(reDatabaseDir.Value().(*model.Scalar).Value(), ShouldEqual, DfsDBPath)
			_, err = ddb.RunScript("n=10")
			So(err, ShouldBeNil)
			_, err = CreateMemTable(ddb, "t", "datev", "id", "sym", "val", "take(2010.01.01..2010.01.31, n)", "[1,4,5,5,6,6,6,6,8,8]", `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`, "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)")
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := CreateDefPartitionedTable(database, "t", DfsTBName1, []string{"id"})
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
			newdfstable, err := LoadPartitionedTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)

			resultDatev = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
			re := resultDatev.String()
			So(re, ShouldEqual, "vector<date>([2010.01.05, 2010.01.06, 2010.01.07, 2010.01.08, 2010.01.01, 2010.01.02, 2010.01.03, 2010.01.04, 2010.01.09, 2010.01.10])")

			resultInt = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			reInt := resultInt.Data.Value()
			tmpInt := []int32{6, 6, 6, 6, 1, 4, 5, 5, 8, 8}
			for i := 0; i < resultInt.Rows(); i++ {
				So(reInt[i], ShouldEqual, tmpInt[i])
			}
			resultSym = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			resym := resultSym.Data.Value()
			tmpSym := []string{"ASZ", "FSD", "BBVC", "AWQ", "AMD", "QWE", "CES", "DOP", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(resym[i], ShouldEqual, tmpSym[i])
			}
			resultVal = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			reVal := resultVal.Data.Value()
			tmpVal := []int32{79, 39, 8, 67, 39, 50, 5, 24, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				So(reVal[i], ShouldEqual, tmpVal[i])
			}
			// create dimensionTable
			_, err = CreateTable(database, "t", TbName1)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			for i := 1; i <= 10; i++ {
				resultDatev = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
				re := resultDatev.Data.Value()
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				So(re[i-1], ShouldEqual, tmp[0])
			}
			resultInt = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			reInt = resultInt.Data.Value()
			tmpInt = []int32{1, 4, 5, 5, 6, 6, 6, 6, 8, 8}
			for i := 0; i < resultInt.Rows(); i++ {
				So(reInt[i], ShouldEqual, tmpInt[i])
			}
			resultSym = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			reSym := resultSym.Data.Value()
			tmpSym = []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(reSym[i], ShouldEqual, tmpSym[i])
			}
			resultVal = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			reVal = resultVal.Data.Value()
			tmpVal = []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				So(reVal[i], ShouldEqual, tmpVal[i])
			}
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re6, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
		Convey("Test_CreateDatabase_olap_list_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			database, err := CreateDatabase(ddb, DfsDBPath, DBhandler, "LIST", "[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS]", "", "", "")
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			re3, err := ddb.RunScript("schema(db)")
			So(err, ShouldBeNil)
			re4 := re3.(*model.Dictionary)
			rePartitionType, _ := re4.Get("partitionType")
			So(rePartitionType.Value().(*model.Scalar).Value(), ShouldEqual, 3)
			reChunkGranularity, _ := re4.Get("chunkGranularity")
			So(reChunkGranularity.Value().(*model.Scalar).Value(), ShouldEqual, "TABLE")
			reAtomic, _ := re4.Get("atomic")
			So(reAtomic.Value().(*model.Scalar).Value(), ShouldEqual, "TRANS")
			rePartitionSites, _ := re4.Get("partitionSites")
			So(rePartitionSites.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
			rePartitionTypeName, _ := re4.Get("partitionTypeName")
			So(rePartitionTypeName.Value().(*model.Scalar).Value(), ShouldEqual, "LIST")
			rePartitionSchema, _ := re4.Get("partitionSchema")
			So(rePartitionSchema.Value().(*model.Vector).String(), ShouldEqual, "vector<any>([vector<string>([AMD, QWE, CES]), vector<string>([DOP, ASZ]), vector<string>([FSD, BBVC]), vector<string>([AWQ, DS])])")
			reDatabaseDir, _ := re4.Get("databaseDir")
			So(reDatabaseDir.Value().(*model.Scalar).Value(), ShouldEqual, DfsDBPath)
			_, err = ddb.RunScript("n=10")
			So(err, ShouldBeNil)
			_, err = CreateMemTable(ddb, "t", "datev", "id", "sym", "val", "take(2010.01.01..2010.01.31, n)", "[1,4,5,5,6,6,6,6,8,8]", `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`, "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)")
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := CreateDefPartitionedTable(database, "t", DfsTBName1, []string{"sym"})
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
			newdfstable, err := LoadPartitionedTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)

			resultDatev = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
			re := resultDatev.String()
			So(re, ShouldEqual, "vector<date>([2010.01.01, 2010.01.02, 2010.01.03, 2010.01.10, 2010.01.04, 2010.01.05, 2010.01.06, 2010.01.07, 2010.01.08, 2010.01.09])")

			resultInt = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			reInt := resultInt.Data.Value()
			tmpInt := []int32{1, 4, 5, 8, 5, 6, 6, 6, 6, 8}
			for i := 0; i < resultInt.Rows(); i++ {
				So(reInt[i], ShouldEqual, tmpInt[i])
			}
			resultSym = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			resym := resultSym.Data.Value()
			tmpSym := []string{"AMD", "QWE", "CES", "AMD", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(resym[i], ShouldEqual, tmpSym[i])
			}
			resultVal = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			reVal := resultVal.Data.Value()
			tmpVal := []int32{39, 50, 5, 55, 24, 79, 39, 8, 67, 29}
			for i := 0; i < resultVal.Rows(); i++ {
				So(reVal[i], ShouldEqual, tmpVal[i])
			}
			// create dimensionTable
			_, err = CreateTable(database, "t", TbName1)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			for i := 1; i <= 10; i++ {
				resultDatev = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
				re := resultDatev.Data.Value()
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				So(re[i-1], ShouldEqual, tmp[0])
			}
			resultInt = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			reInt = resultInt.Data.Value()
			tmpInt = []int32{1, 4, 5, 5, 6, 6, 6, 6, 8, 8}
			for i := 0; i < resultInt.Rows(); i++ {
				So(reInt[i], ShouldEqual, tmpInt[i])
			}
			resultSym = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			reSym := resultSym.Data.Value()
			tmpSym = []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(reSym[i], ShouldEqual, tmpSym[i])
			}
			resultVal = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			reVal = resultVal.Data.Value()
			tmpVal = []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				So(reVal[i], ShouldEqual, tmpVal[i])
			}
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re6, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
		Convey("Test_CreateDatabase_olap_compo_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			_, err = CreateDatabase(ddb, "", "db1", "VALUE", "2010.01.01..2010.01.30", "", "", "")
			So(err, ShouldBeNil)
			_, err = CreateDatabase(ddb, "", "db2", "RANGE", "1 3 5 7 9 10", "", "", "")
			So(err, ShouldBeNil)
			database, err := CreateDatabase(ddb, DfsDBPath, DBhandler, "COMPO", "[db1, db2]", "", "", "")
			So(err, ShouldBeNil)
			re2, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re2, ShouldBeTrue)
			re3, err := ddb.RunScript("schema(db)")
			So(err, ShouldBeNil)
			re4 := re3.(*model.Dictionary)
			rePartitionType, _ := re4.Get("partitionType")
			So(rePartitionType.Value().(*model.Vector).String(), ShouldEqual, "vector<int>([1, 2])")
			reChunkGranularity, _ := re4.Get("chunkGranularity")
			So(reChunkGranularity.Value().(*model.Scalar).Value(), ShouldEqual, "TABLE")
			reAtomic, _ := re4.Get("atomic")
			So(reAtomic.Value().(*model.Scalar).Value(), ShouldEqual, "TRANS")
			rePartitionSites, _ := re4.Get("partitionSites")
			So(rePartitionSites.Value().(*model.Scalar).IsNull(), ShouldBeTrue)
			rePartitionTypeName, _ := re4.Get("partitionTypeName")
			So(rePartitionTypeName.Value().(*model.Vector).String(), ShouldEqual, "vector<string>([VALUE, RANGE])")
			rePartitionSchema, _ := re4.Get("partitionSchema")
			re := rePartitionSchema.Value().(*model.Vector).Get(0).Value().(*model.Vector).Data.Value()
			j := 0
			for i := 1; i < 30; i++ {
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				So(re[j], ShouldEqual, tmp[0])
				j++
			}
			So(rePartitionSchema.Value().(*model.Vector).Get(1).Value().(*model.Vector).String(), ShouldEqual, "vector<int>([1, 3, 5, 7, 9, 10])")
			reDatabaseDir, _ := re4.Get("databaseDir")
			So(reDatabaseDir.Value().(*model.Scalar).Value(), ShouldEqual, DfsDBPath)
			_, err = ddb.RunScript("n=10")
			So(err, ShouldBeNil)
			_, err = CreateMemTable(ddb, "t", "datev", "id", "sym", "val", "take(2010.01.01..2010.01.31, n)", "[1,4,5,5,6,6,6,6,8,8]", `take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n)`, "take([39, 50, 5, 24, 79, 39, 8, 67, 29, 55], n)")
			So(err, ShouldBeNil)
			// create dfsTable
			dfsTable, err := CreateDefPartitionedTable(database, "t", DfsTBName1, []string{"datev", "id"})
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
			newdfstable, err := LoadPartitionedTable(ddb, DfsTBName1, DfsDBPath)
			So(err, ShouldBeNil)

			resultDatev = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
			reDate := resultDatev.Data.Value()
			for i := 1; i < resultDatev.Rows(); i++ {
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				So(reDate[i-1], ShouldEqual, tmp[0])
			}
			resultInt = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			reInt := resultInt.Data.Value()
			tmpInt := []int32{1, 4, 5, 5, 6, 6, 6, 6, 8, 8}
			for i := 0; i < resultInt.Rows(); i++ {
				So(reInt[i], ShouldEqual, tmpInt[i])
			}
			resultSym = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			resym := resultSym.Data.Value()
			tmpSym := []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(resym[i], ShouldEqual, tmpSym[i])
			}
			resultVal = newdfstable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			reVal := resultVal.Data.Value()
			tmpVal := []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				So(reVal[i], ShouldEqual, tmpVal[i])
			}
			// create dimensionTable
			_, err = CreateTable(database, "t", TbName1)
			So(err, ShouldBeNil)
			_, err = ddb.RunScript(`select * from loadTable("` + DfsDBPath + `", "` + TbName1 + `").append!(t)`)
			So(err, ShouldBeNil)
			dimensionTable, err := LoadTable(ddb, TbName1, DfsDBPath)
			So(err, ShouldBeNil)
			for i := 1; i <= 10; i++ {
				resultDatev = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[0])
				re := resultDatev.Data.Value()
				datev := time.Date(2010, time.January, i, 0, 0, 0, 0, time.UTC)
				tmp := []time.Time{datev}
				So(re[i-1], ShouldEqual, tmp[0])
			}
			resultInt = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[1])
			reInt = resultInt.Data.Value()
			tmpInt = []int32{1, 4, 5, 5, 6, 6, 6, 6, 8, 8}
			for i := 0; i < resultInt.Rows(); i++ {
				So(reInt[i], ShouldEqual, tmpInt[i])
			}
			resultSym = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[2])
			reSym := resultSym.Data.Value()
			tmpSym = []string{"AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS", "AMD"}
			for i := 0; i < resultSym.Rows(); i++ {
				So(reSym[i], ShouldEqual, tmpSym[i])
			}
			resultVal = dimensionTable.Data.GetColumnByName(dfsTable.Data.GetColumnNames()[3])
			reVal = resultVal.Data.Value()
			tmpVal = []int32{39, 50, 5, 24, 79, 39, 8, 67, 29, 55}
			for i := 0; i < resultVal.Rows(); i++ {
				So(reVal[i], ShouldEqual, tmpVal[i])
			}
			err = DropDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			re6, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re6, ShouldBeFalse)
		})
	})
}

func TestDataBaseGetSession(t *testing.T) {
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
		Convey("Test_CreateDatabase_olap_value_partition", func() {
			DfsDBPath := "dfs://" + generateRandomString(8)
			re1, err := ExistsDatabase(ddb, DfsDBPath)
			So(err, ShouldBeNil)
			So(re1, ShouldBeFalse)
			database, err := CreateDatabase(ddb, DfsDBPath, DBhandler, "VALUE", "2010.01.01..2010.01.30", "", "", "")
			So(err, ShouldBeNil)
			res := database.GetSession()
			So(res, ShouldNotBeNil)
		})
	})
}
