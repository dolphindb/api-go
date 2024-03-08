package test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/streaming"
	"github.com/dolphindb/api-go/test/setup"
)

var (
	DfsDBPath    = "dfs://test_dfsTable"
	TbName1      = "tb1"
	TbName2      = "tb2"
	DfsTBName1   = "pt1"
	DfsTBName2   = "pt2"
	DiskDBPath   = setup.WORK_DIR + `/testTable`
	DBhandler    = "db"
	MemTableName = "memTable"
)

func AssertNil(err error) {
	if err != nil {
		panic(fmt.Sprintf("err is not nil: %s", err.Error()))
	}
}

func AssertEqual(s, d interface{}) {
	if !reflect.DeepEqual(s, d) {
		panic(fmt.Sprintf("%v != %v", s, d))
	}
}

func LoadTextFileName(ddb api.DolphinDB, remoteFilePath string) (*api.Table, error) {
	t := new(api.LoadTextRequest).
		SetFileName(remoteFilePath)
	di, err := ddb.LoadText(t)
	return di, err
}

func LoadTextDelimiter(ddb api.DolphinDB, remoteFilePath string, delimiter string) (*api.Table, error) {
	t := new(api.LoadTextRequest).
		SetFileName(remoteFilePath).SetDelimiter(delimiter)
	di, err := ddb.LoadText(t)
	return di, err
}

func PloadTextFileName(ddb api.DolphinDB, remoteFilePath string) (*api.Table, error) {
	t := new(api.PloadTextRequest).
		SetFileName(remoteFilePath)
	di, err := ddb.PloadText(t)
	return di, err
}

func PloadTextDelimiter(ddb api.DolphinDB, remoteFilePath string, delimiter string) (*api.Table, error) {
	t := new(api.PloadTextRequest).
		SetFileName(remoteFilePath).SetDelimiter(delimiter)
	di, err := ddb.PloadText(t)
	return di, err
}

func CompareTablesDataformTable(tableName1 *model.Table, tableName2 *api.Table) bool {
	re2 := tableName2.Data
	if tableName1.Columns() == re2.Columns() && tableName1.GetDataTypeString() == re2.GetDataTypeString() && tableName1.GetDataForm() == re2.GetDataForm() {
		for i := 0; i < tableName1.Columns(); i++ {
			reTable1 := tableName1.GetColumnByName(tableName1.GetColumnNames()[i]).Data.Value()
			reTable2 := tableName2.Data.GetColumnByName(tableName2.Data.GetColumnNames()[i]).Data.Value()
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

func CompareTables(tableName1 *api.Table, tableName2 *api.Table) bool {
	re1 := tableName1.Data
	re2 := tableName2.Data
	if re1.Columns() == re2.Columns() && re1.GetDataTypeString() == re2.GetDataTypeString() && re1.GetDataForm() == re2.GetDataForm() {
		for i := 0; i < tableName1.Data.Columns(); i++ {
			reTable1 := tableName1.Data.GetColumnByName(tableName1.Data.GetColumnNames()[i]).Data.Value()
			reTable2 := tableName2.Data.GetColumnByName(tableName2.Data.GetColumnNames()[i]).Data.Value()
			for i := 0; i < tableName1.Data.Rows(); i++ {
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

func DropDatabase(ddb api.DolphinDB, dbPath string) error {
	t := new(api.DropDatabaseRequest).
		SetDirectory(dbPath)
	err := ddb.DropDatabase(t)
	return err
}

func ExistsDatabase(ddb api.DolphinDB, dbPath string) (bool, error) {
	d := new(api.ExistsDatabaseRequest).
		SetPath(dbPath)
	b, err := ddb.ExistsDatabase(d)
	return b, err
}

func CreateMemTable(ddb api.DolphinDB, tableName string, colName1 string, colName2 string, colName3 string, colName4 string, dataList1 string, dataList2 string, dataList3 string, dataList4 string) (*api.Table, error) {
	l := new(api.TableRequest).
		SetTableName(tableName).
		AddTableParam(colName1, dataList1).
		AddTableParam(colName2, dataList2).
		AddTableParam(colName3, dataList3).
		AddTableParam(colName4, dataList4)
	t, err := ddb.Table(l)
	return t, err
}

func CreateTableWithCapacity(ddb api.DolphinDB, tableName string, capcity int32, size int32, colName []string, typeName []string) (*api.Table, error) {
	l := new(api.TableWithCapacityRequest).
		SetTableName(tableName).SetCapacity(capcity).SetSize(size).
		SetColNames(colName).
		SetColTypes(typeName)
	t, err := ddb.TableWithCapacity(l)
	return t, err
}

func ExistsTable(ddb api.DolphinDB, dbPath string, tableName string) (bool, error) {
	l := new(api.ExistsTableRequest).
		SetDBPath(dbPath).
		SetTableName(tableName)
	b, err := ddb.ExistsTable(l)
	return b, err
}

func SaveTable(ddb api.DolphinDB, dbPath string, tableName string, dbhandler string) error {
	l := new(api.SaveTableRequest).
		SetTableName(tableName).
		SetDBPath(dbPath).
		SetDBHandle(dbhandler)
	err := ddb.SaveTable(l)
	return err
}

func DropTable(ddb api.DolphinDB, tableName string, dfsDBPath string) error {
	t := new(api.DropTableRequest).
		SetTableName(tableName).
		SetDBPath(dfsDBPath)
	err := ddb.DropTable(t)
	return err
}

func LoadTable(ddb api.DolphinDB, tableName string, dbPath string) (*api.Table, error) {
	t := new(api.LoadTableRequest).
		SetTableName(tableName).
		SetDatabase(dbPath)
	df, err := ddb.LoadTable(t)
	return df, err
}

func LoadTablePartitions(ddb api.DolphinDB, tableName string, dbPath string, partitions string) (*api.Table, error) {
	t := new(api.LoadTableRequest).
		SetTableName(tableName).
		SetDatabase(dbPath).
		SetPartitions(partitions)
	df, err := ddb.LoadTable(t)
	return df, err
}

func LoadTableMemoryMode(ddb api.DolphinDB, tableName string, dbPath string, memoryMode bool) (*api.Table, error) {
	t := new(api.LoadTableRequest).
		SetTableName(tableName).
		SetDatabase(dbPath).
		SetMemoryMode(memoryMode)
	df, err := ddb.LoadTable(t)
	return df, err
}

func LoadTableBySQL(ddb api.DolphinDB, na string, loadSQL string, dbPath string, partitionedTableName string) (*api.Table, error) {
	t := new(api.LoadTableBySQLRequest).
		SetSQL(fmt.Sprintf(loadSQL, na)).
		SetDBPath(dbPath).
		SetTableName(partitionedTableName)
	df, err := ddb.LoadTableBySQL(t)
	return df, err
}

func Database(ddb api.DolphinDB, dbPath string, dbhandler string) (*api.Database, error) {
	d := new(api.DatabaseRequest).
		SetDirectory(dbPath).
		SetDBHandle(dbhandler)
	dt, err := ddb.Database(d)
	return dt, err
}

func CreateDatabase(ddb api.DolphinDB, dbPath string, dbhandler string, partitionType string, partitionScheme string, location string, engineType string, atomic string) (*api.Database, error) {
	d := new(api.DatabaseRequest).
		SetDBHandle(dbhandler).
		SetDirectory(dbPath).
		SetPartitionType(partitionType).
		SetPartitionScheme(partitionScheme).
		SetEngine(engineType).
		SetLocations(location).
		SetAtomic(atomic)
	dt, err := ddb.Database(d)
	return dt, err
}

func CreateTable(db *api.Database, tableName string, dimensionTableName string) (*api.Table, error) {
	c := new(api.CreateTableRequest).
		SetSrcTable(tableName).
		SetDimensionTableName(dimensionTableName)
	t, err := db.CreateTable(c)
	return t, err
}

func CreateDefPartitionedTable(ddb *api.Database, tableName string, partitionedTableName string, partitioncolumns []string) (*api.Table, error) {
	c := new(api.CreatePartitionedTableRequest).
		SetSrcTable(tableName).
		SetPartitionedTableName(partitionedTableName).
		SetPartitionColumns(partitioncolumns)
	t, err := ddb.CreatePartitionedTable(c)
	return t, err
}

func DropPartition(db api.DolphinDB, partitionedTableName string, dbPath string, partitionPaths string) error {
	t := new(api.DropPartitionRequest).
		SetPartitionPaths(partitionPaths).
		SetDBPath(dbPath).
		SetTableName(partitionedTableName)
	err := db.DropPartition(t)
	return err
}

func LoadPartitionedTable(db api.DolphinDB, partitionedTableName string, dbPath string) (*api.Table, error) {
	t := new(api.LoadTableRequest).
		SetTableName(partitionedTableName).
		SetDatabase(dbPath)
	df, err := db.LoadTable(t)
	return df, err
}

func CreateDfsDimensiondb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, RANGE, 1..10)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createTable(tdata, "` + tableName1 + `").append!(tdata)
    db.createTable(tdata, "` + tableName2 + `").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsRangedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, RANGE, 0..10*10000+1)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","id").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","id").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsRangedbChunkGranularity(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, RANGE, 0..10*10000+1, chunkGranularity="DATABASE")
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","id").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","id").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsHashdb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, HASH, [INT,10])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n) as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","id").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","id").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsHashdbChunkGranularity(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, HASH, [INT,10], chunkGranularity="DATABASE")
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n) as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","id").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","id").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsValuedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
    db=database(dbPath, VALUE, 2010.01.01..2010.01.30)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","date").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","date").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsValuedbChunkGranularity(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
    db=database(dbPath, VALUE, 2010.01.01..2010.01.30, chunkGranularity="DATABASE")
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","date").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","date").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsListdb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, LIST, [["AMD", "QWE", "CES"],["DOP", "ASZ"],["FSD", "BBVC"],["AWQ", "DS"]])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","sym").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","sym").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsListdbChunkGranularity(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, LIST, [["AMD", "QWE", "CES"],["DOP", "ASZ"],["FSD", "BBVC"],["AWQ", "DS"]], chunkGranularity="DATABASE")
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `","sym").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","sym").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsCompoRangeRangedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
		db1=database('', RANGE, 2010.01M+0..12)
		db2=database('', RANGE, 1 3 5 7 9 11)
		db=database(dbPath, COMPO, [db1,db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n) as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `",["date", "id"]).append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `",["date", "id"]).append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsCompoRangeRangedbChunkGranularity(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
		db1=database('', RANGE, 2010.01M+0..12)
		db2=database('', RANGE, 1 3 5 7 9 11)
		db=database(dbPath, COMPO, [db1,db2], chunkGranularity="DATABASE")
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n) as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `",["date", "id"]).append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `",["date", "id"]).append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsCompoRangeValuedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
		db1=database('', RANGE, 0..10*10000+1)
		db2=database('', VALUE, 2010.01.01..2010.01.30)
		db=database(dbPath, COMPO, [db1, db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `",["id", "date"]).append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `",["id", "date"]).append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsCompoRangeHashdb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
		db1=database('', RANGE, 2010.01M+0..12)
		db2=database('', HASH, [INT, 10])
		db=database(dbPath, COMPO, [db1, db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `",["date", "id"]).append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `",["date", "id"]).append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDfsCompoRangeListdb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(existsDatabase(dbPath)){dropDatabase(dbPath)}
		db1=database('', RANGE, 2010.01M+0..12)
		db2=database('', LIST, ["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"])
		db=database(dbPath, COMPO, [db1, db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
    db.createPartitionedTable(tdata,"` + tableName1 + `",["date", "sym"]).append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `",["date", "sym"]).append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDiskUnpartitioneddb(addr string, dbPath string, tbName1 string, tbName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(exists(dbPath)){rmdir(dbPath, true)}
		db=database(dbPath)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
		saveTable(db, tdata, "` + tbName1 + `")
		saveTable(db, tdata, "` + tbName2 + `")
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDiskRangedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(exists(dbPath)){rmdir(dbPath, true)}
		db=database(dbPath, RANGE, 0..10*10000+1)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
		db.createPartitionedTable(tdata,"` + tableName1 + `","id").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","id").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDiskHashdb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(exists(dbPath)){rmdir(dbPath, true)}
		db=database(dbPath, HASH, [INT, 10])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n) as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
		db.createPartitionedTable(tdata,"` + tableName1 + `","id").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","id").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDiskValuedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(exists(dbPath)){rmdir(dbPath, true)}
		db=database(dbPath, VALUE, 2010.01.01..2010.01.30)
    n=100000
    tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10, n)  as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
		db.createPartitionedTable(tdata,"` + tableName1 + `","date").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","date").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDiskListdb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(exists(dbPath)){rmdir(dbPath, true)}
		db=database(dbPath,LIST,[["AMD", "QWE", "CES"],["DOP", "ASZ"],["FSD", "BBVC"],["AWQ", "DS"]])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n)  as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
		db.createPartitionedTable(tdata,"` + tableName1 + `","sym").append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `","sym").append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func CreateDiskCompoRangeRangedb(addr string, dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	ddbScript := `
    dbPath="` + dbPath + `"
    if(exists(dbPath)){rmdir(dbPath, true)}
		db1=database('', RANGE, 2010.01M+0..12)
    db2=database('', RANGE, 1 3 5 7 9 11)
    db=database(dbPath, COMPO, [db1, db2])
    n=100000
    tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10, n)  as id, take(["AMD", "QWE", "CES", "DOP", "ASZ", "FSD", "BBVC", "AWQ", "DS"], n) as sym, rand(100, n) as val)
		db.createPartitionedTable(tdata,"` + tableName1 + `",["date", "id"]).append!(tdata)
		db.createPartitionedTable(tdata,"` + tableName2 + `",["date", "id"]).append!(tdata)
    `
	_, err = ddb.RunScript(ddbScript)
	AssertNil(err)
	errClose := ddb.Close()
	AssertNil(errClose)
}

func SaveText(ddb api.DolphinDB, obj string, remoteFilePath string) error {
	t := new(api.SaveTextRequest).
		SetFileName(remoteFilePath).
		SetObj(obj)
	err := ddb.SaveText(t)
	return err
}

func CreateDBConnectionPool(addr string, threadNumCount int, loadbalance bool) *api.DBConnectionPool {
	opt := &api.PoolOption{
		Address:     addr,
		UserID:      setup.UserName,
		Password:    setup.Password,
		PoolSize:    threadNumCount,
		LoadBalance: loadbalance,
	}
	pool, err := api.NewDBConnectionPool(opt)
	AssertNil(err)
	return pool
}

func ClearEnv(addr string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	_, err = ddb.RunScript("a = getStreamingStat().pubTables\n" +
		"for(i in a){\n" +
		"\ttry{stopPublishTable(i.subscriber.split(\":\")[0],int(i.subscriber.split(\":\")[1]),i.tableName,i.actions)}catch(ex){}\n" +
		"}")
	AssertNil(err)
	_, err = ddb.RunScript("def getAllShare(){\n" +
		"\treturn select name from objs(true) where shared=1\n" +
		"\t}\n" +
		"\n" +
		"def clearShare(){\n" +
		"\tlogin(`admin,`123456)\n" +
		"\tallShare=exec name from pnodeRun(getAllShare)\n" +
		"\tfor(i in allShare){\n" +
		"\t\ttry{\n" +
		"\t\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],clearTablePersistence,objByName(i))\n" +
		"\t\t\t}catch(ex1){}\n" +
		"\t\trpc((exec node from pnodeRun(getAllShare) where name =i)[0],undef,i,SHARED)\n" +
		"\t}\n" +
		"\ttry{\n" +
		"\t\tPST_DIR=rpc(getControllerAlias(),getDataNodeConfig{getNodeAlias()})['persistenceDir']\n" +
		"\t}catch(ex1){}\n" +
		"}\n" +
		"clearShare()")
	AssertNil(err)
	err = ddb.Close()
	AssertNil(err)
}

func ClearStreamTable(addr string, tableName string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), addr, setup.UserName, setup.Password)
	AssertNil(err)
	script := "login(`admin,`123456);" +
		"dropStreamTable('" + tableName + "');go"
	_, err = ddb.RunScript(script)
	AssertNil(err)
	err = ddb.Close()
	AssertNil(err)
}

func CheckmodelTableEqual(t1 *model.Table, t2 *model.Table, n int) bool {
	for i := 0; i < t1.Rows(); i++ {
		for j := 0; j < len(t1.GetColumnNames()); j++ {
			if t1.GetColumnByIndex(j).Get(i).Value() != t2.GetColumnByIndex(j).Get(n+i).Value() {
				return false
			}
		}
	}
	return true
}

func CheckmodelTableEqual_throttle(t1 *model.Table, t2 *model.Table, m int, n int) bool {
	for i := 0; i < 1000; i++ {
		for j := 0; j < len(t1.GetColumnNames()); j++ {
			if t1.GetColumnByIndex(j).Get(i+m).Value() != t2.GetColumnByIndex(j).Get(n+i).Value() {
				return false
			}
		}
	}
	return true
}

func getRandomStr(length int) string {
	// 定义字符集
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(letterRunes))))
		if err != nil {
			panic(err)
		}
		b[i] = letterRunes[n.Int64()]
	}
	return string(b)
}

func CreateStreamingTableWithRandomName(conn api.DolphinDB) (string, string) {
	suffix := getRandomStr(5)
	_, err := conn.RunScript("login(`admin,`123456);" +
		"try{dropStreamTable('st1')}catch(ex){};" +
		"try{dropStreamTable('st2')}catch(ex){};")
	AssertNil(err)
	st := "Trades_" + suffix
	re := "Receive_" + suffix
	_, err = conn.RunScript("st1 = streamTable(1:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
		"share(st1,`" + st + ")\t\n" + "setStreamTableFilterColumn(objByName(`" + st + "),`tag)")
	AssertNil(err)
	_, err = conn.RunScript("st2 = streamTable(1:0,`tag`ts`data,[INT,TIMESTAMP,DOUBLE])\n" +
		"share(st2, `" + re + ")\t\n")
	AssertNil(err)
	return st, re
}

var wg sync.WaitGroup

func threadWriteData(conn api.DolphinDB, tabName string, batch int) {
	defer wg.Done()
	for i := 0; i < batch; i++ {
		_, err := conn.RunScript("n=1000;t=table(1..n as tag,now()+1..n as ts,rand(100.0,n) as data);" + tabName + ".append!(t);sleep(10)")
		AssertNil(err)
		time.Sleep(1 * time.Second)
	}
}

func waitData(conn api.DolphinDB, tableName string, dataRow int) {
	loop := 0
	for {
		loop += 1
		if loop > 60 {
			panic("wait for subscribe datas timeout.")
		}
		tmp, err := conn.RunScript("exec count(*) from " + tableName)
		AssertNil(err)
		rowNum := tmp.(*model.Scalar)
		fmt.Printf("\nexpectedData is: %v", dataRow)
		fmt.Printf("\nactualData is: %v", rowNum)
		if dataRow == int(rowNum.Value().(int32)) {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

type MessageBatchHandler struct {
	receive string
	conn    api.DolphinDB
}
type MessageHandler struct {
	receive string
	conn    api.DolphinDB
}
type MessageHandler_table struct {
	receive string
	conn    api.DolphinDB
}

type MessageHandler_unsubscribeInDoEvent struct {
	subType      string
	subClient    interface{}
	subReq       *streaming.SubscribeRequest
	successCount int
}

type sdHandler struct {
	sd         streaming.StreamDeserializer
	msg1_total int
	msg2_total int
	res1_data  []*model.Vector
	res2_data  []*model.Vector
	coltype1   []model.DataTypeByte
	coltype2   []model.DataTypeByte
	lock       *sync.Mutex
}

type sdBatchHandler struct {
	sd         streaming.StreamDeserializer
	msg1_total int
	msg2_total int
	res1_data  []*model.Vector
	res2_data  []*model.Vector
	coltype1   []model.DataTypeByte
	coltype2   []model.DataTypeByte
	lock       *sync.Mutex
}

type sdHandler_av struct {
	sd         streaming.StreamDeserializer
	msg1_total int
	msg2_total int
	res1_data  []*model.Vector
	res2_data  []*model.Vector
	coltype1   []model.DataTypeByte
	coltype2   []model.DataTypeByte
	lock       *sync.Mutex
}

type sdBatchHandler_av struct {
	sd         streaming.StreamDeserializer
	msg1_total int
	msg2_total int
	res1_data  []*model.Vector
	res2_data  []*model.Vector
	coltype1   []model.DataTypeByte
	coltype2   []model.DataTypeByte
	lock       *sync.Mutex
}

func (s *MessageBatchHandler) DoEvent(msgv []streaming.IMessage) {
	for _, msg := range msgv {
		val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
		val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
		val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
		script := fmt.Sprintf("tableInsert(objByName(`"+s.receive+", true), %s,%s,%s)",
			val0, val1, val2)
		_, err := s.conn.RunScript(script)
		AssertNil(err)
	}
}

func (s *MessageHandler) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Scalar).DataType.String()
	val1 := msg.GetValue(1).(*model.Scalar).DataType.String()
	val2 := msg.GetValue(2).(*model.Scalar).DataType.String()
	script := fmt.Sprintf("tableInsert(objByName(`"+s.receive+", true), %s,%s,%s)",
		val0, val1, val2)
	_, err := s.conn.RunScript(script)
	AssertNil(err)
}

func (s *MessageHandler_table) DoEvent(msg streaming.IMessage) {
	val0 := msg.GetValue(0).(*model.Vector)
	val1 := msg.GetValue(1).(*model.Vector)
	val2 := msg.GetValue(2).(*model.Vector)

	for i := 0; i < len(val0.Data.Value()); i++ {
		script := fmt.Sprintf("tableInsert(objByName(`"+s.receive+", true), %s,%s,%s)",
			val0.Data.Get(i).String(), val1.Data.Get(i).String(), val2.Data.Get(i).String())
		_, err := s.conn.RunScript(script)
		AssertNil(err)
	}
}

func (s *MessageHandler_unsubscribeInDoEvent) DoEvent(msg streaming.IMessage) {
	time.Sleep(3 * time.Second)
	var err error
	if s.subType == "gc" {
		err = s.subClient.(*streaming.GoroutineClient).UnSubscribe(s.subReq)
	} else if s.subType == "gpc" {
		err = s.subClient.(*streaming.GoroutinePooledClient).UnSubscribe(s.subReq)
	} else if s.subType == "pc" {
		err = s.subClient.(*streaming.PollingClient).UnSubscribe(s.subReq)
	}
	if err == nil {
		s.successCount += 1
	}
}

func (s *sdHandler) DoEvent(msg streaming.IMessage) {
	ret, err := s.sd.Parse(msg)
	AssertNil(err)
	sym := ret.GetSym()

	s.lock.Lock()
	if sym == "msg1" {
		s.msg1_total += 1
		AssertEqual(ret.Size(), 5)
		for i := 0; i < len(s.coltype1); i++ {
			AssertEqual(ret.GetValue(i).GetDataType(), s.coltype1[i])
			// fmt.Println(ret.GetValue(i).(*model.Scalar).Value())
			val := ret.GetValue(i).(*model.Scalar).Value()
			dt, err := model.NewDataType(s.coltype1[i], val)
			AssertNil(err)
			AssertNil(s.res1_data[i].Append(dt))
		}

	} else if sym == "msg2" {
		s.msg2_total += 1
		AssertEqual(ret.Size(), 4)
		for i := 0; i < len(s.coltype2); i++ {
			AssertEqual(ret.GetValue(i).GetDataType(), s.coltype2[i])
			// fmt.Println(ret.GetValue(i).GetDataType(), ex_types2[i])
			val := ret.GetValue(i).(*model.Scalar).Value()
			dt, err := model.NewDataType(s.coltype2[i], val)
			AssertNil(err)
			AssertNil(s.res2_data[i].Append(dt))
		}
	}
	s.lock.Unlock()
}

func (s *sdBatchHandler) DoEvent(msgs []streaming.IMessage) {
	for _, msg := range msgs {
		ret, err := s.sd.Parse(msg)
		AssertNil(err)
		sym := ret.GetSym()

		s.lock.Lock()
		if sym == "msg1" {
			s.msg1_total += 1
			AssertEqual(ret.Size(), 5)
			for i := 0; i < len(s.coltype1); i++ {
				AssertEqual(ret.GetValue(i).GetDataType(), s.coltype1[i])
				// fmt.Println(ret.GetValue(i).(*model.Scalar).Value())
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype1[i], val)
				AssertNil(err)
				AssertNil(s.res1_data[i].Append(dt))
			}

		} else if sym == "msg2" {
			s.msg2_total += 1
			AssertEqual(ret.Size(), 4)
			for i := 0; i < len(s.coltype2); i++ {
				AssertEqual(ret.GetValue(i).GetDataType(), s.coltype2[i])
				// fmt.Println(ret.GetValue(i).GetDataType(), ex_types2[i])
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype2[i], val)
				AssertNil(err)
				AssertNil(s.res2_data[i].Append(dt))
			}

		}
		s.lock.Unlock()
	}

}

func (s *sdHandler_av) DoEvent(msg streaming.IMessage) {
	ret, err := s.sd.Parse(msg)
	AssertNil(err)
	sym := ret.GetSym()

	s.lock.Lock()
	if sym == "msg1" {
		s.msg1_total += 1
		AssertEqual(ret.Size(), 5)
		for i := 0; i < len(s.coltype1); i++ {
			AssertEqual(ret.GetValue(i).GetDataType(), s.coltype1[i])
			fmt.Println(ret.GetValue(i).GetDataFormString())
			if i != 3 {
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype1[i], val)
				AssertNil(err)
				AssertNil(s.res1_data[i].Append(dt))
			} else {
				val := ret.GetValue(i).(*model.Vector)
				dt, err := model.NewDataType(s.coltype1[i], val)
				AssertNil(err)
				AssertNil(s.res1_data[i].Append(dt))
			}
		}

	} else if sym == "msg2" {
		s.msg2_total += 1
		AssertEqual(ret.Size(), 4)
		for i := 0; i < len(s.coltype2); i++ {
			AssertEqual(ret.GetValue(i).GetDataType(), s.coltype2[i])
			fmt.Println(ret.GetValue(i).GetDataFormString())
			val := ret.GetValue(i).(*model.Scalar).Value()
			dt, err := model.NewDataType(s.coltype2[i], val)
			AssertNil(err)
			AssertNil(s.res2_data[i].Append(dt))
		}
	}
	s.lock.Unlock()
}

func (s *sdBatchHandler_av) DoEvent(msgs []streaming.IMessage) {
	for _, msg := range msgs {
		ret, err := s.sd.Parse(msg)
		AssertNil(err)
		sym := ret.GetSym()

		s.lock.Lock()
		if sym == "msg1" {
			s.msg1_total += 1
			AssertEqual(ret.Size(), 5)
			for i := 0; i < len(s.coltype1); i++ {
				AssertEqual(ret.GetValue(i).GetDataType(), s.coltype1[i])
				// fmt.Println(ret.GetValue(i).(*model.Scalar).Value())
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype1[i], val)
				AssertNil(err)
				AssertNil(s.res1_data[i].Append(dt))
			}

		} else if sym == "msg2" {
			s.msg2_total += 1
			AssertEqual(ret.Size(), 4)
			for i := 0; i < len(s.coltype2); i++ {
				AssertEqual(ret.GetValue(i).GetDataType(), s.coltype2[i])
				// fmt.Println(ret.GetValue(i).GetDataType(), ex_types2[i])
				val := ret.GetValue(i).(*model.Scalar).Value()
				dt, err := model.NewDataType(s.coltype2[i], val)
				AssertNil(err)
				AssertNil(s.res2_data[i].Append(dt))
			}

		}
		s.lock.Unlock()
	}

}

func createStreamDeserializer(conn api.DolphinDB, tbname string) (sdHandler, sdBatchHandler) {
	_, err := conn.RunScript("try{ dropStreamTable(`" + tbname + ");}catch(ex){};" +
		"try{ dropStreamTable(`st2);}catch(ex){};" +
		"try{ undef(`table1, SHARED);}catch(ex){};" +
		"try{ undef(`table2, SHARED);}catch(ex){};go;" +
		`st2 = streamTable(100:0, 'timestampv''sym''blob''price1',[TIMESTAMP,SYMBOL,BLOB,DOUBLE]);
		enableTableShareAndPersistence(table=st2, tableName='` + tbname + `', asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);
		go;
		setStreamTableFilterColumn(` + tbname + `, 'sym')`)
	AssertNil(err)
	_, err = conn.RunScript(
		`n = 1000;
		t0 = table(100:0, "datetimev""timestampv""sym""price1""price2", [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
		share t0 as table1;
		t = table(100:0, "datetimev""timestampv""sym""price1", [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
		tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take("a1""b1""c1",n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
		tableInsert(t, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take("a1""b1""c1",n), rand(100,n)+rand(1.0, n));
		dbpath="dfs://test_dfs";if(existsDatabase(dbpath)){dropDatabase(dbpath)};db=database(dbpath, VALUE, "a1""b1""c1");
		db.createPartitionedTable(t,"table2","sym").append!(t);
		t2 = select * from loadTable(dbpath,"table2");share t2 as table2;
		d = dict(['msg1','msg2'], [table1, table2]);
		replay(inputTables=d, outputTables="` + tbname + `", dateColumn="timestampv", timeColumn="timestampv")`)
	AssertNil(err)
	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "table1"}
	sdMap["msg2"] = [2]string{"dfs://test_dfs", "table2"}
	opt := streaming.StreamDeserializerOption{
		TableNames: sdMap,
		Conn:       conn,
	}
	sd, err := streaming.NewStreamDeserializer(&opt)
	AssertNil(err)
	ex_types1 := []model.DataTypeByte{model.DtDatetime, model.DtTimestamp, model.DtSymbol, model.DtDouble, model.DtDouble}
	args1 := make([]*model.Vector, 5)
	args1[0] = model.NewVector(model.NewDataTypeList(ex_types1[0], []model.DataType{}))
	args1[1] = model.NewVector(model.NewDataTypeList(ex_types1[1], []model.DataType{}))
	args1[2] = model.NewVector(model.NewDataTypeList(ex_types1[2], []model.DataType{}))
	args1[3] = model.NewVector(model.NewDataTypeList(ex_types1[3], []model.DataType{}))
	args1[4] = model.NewVector(model.NewDataTypeList(ex_types1[4], []model.DataType{}))
	ex_types2 := []model.DataTypeByte{model.DtDatetime, model.DtTimestamp, model.DtSymbol, model.DtDouble}
	args2 := make([]*model.Vector, 4)
	args2[0] = model.NewVector(model.NewDataTypeList(ex_types2[0], []model.DataType{}))
	args2[1] = model.NewVector(model.NewDataTypeList(ex_types2[1], []model.DataType{}))
	args2[2] = model.NewVector(model.NewDataTypeList(ex_types2[2], []model.DataType{}))
	args2[3] = model.NewVector(model.NewDataTypeList(ex_types2[3], []model.DataType{}))

	var lock1 sync.Mutex
	var lock2 sync.Mutex
	plock1 := &lock1
	plock2 := &lock2
	sh := sdHandler{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock1}
	sbh := sdBatchHandler{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock2}
	fmt.Println("create handler successfully.")
	return sh, sbh
}

func createStreamDeserializer_av(conn api.DolphinDB, tbname string, dataType model.DataTypeByte, vecVal string) (sdHandler_av, sdBatchHandler_av) {
	typeString := strings.ToUpper(model.GetDataTypeString(dataType))
	if strings.Contains(typeString, "DECIMAL32") {
		typeString = "DECIMAL32(5)"
	} else if strings.Contains(typeString, "DECIMAL64") {
		typeString = "DECIMAL64(15)"
	} else if strings.Contains(typeString, "DECIMAL128") {
		typeString = "DECIMAL128(33)"
	}
	typeString = typeString + "[]"
	fmt.Println(`test type: `, typeString)

	_, err := conn.RunScript("login(\"admin\",\"123456\");" +
		"try{ dropStreamTable(`" + tbname + ");}catch(ex){};" +
		"try{ dropStreamTable(`st2);}catch(ex){};" +
		"try{ undef(`table1, SHARED);}catch(ex){};" +
		"try{ undef(`table2, SHARED);}catch(ex){};go;" +
		"st2 = streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB," + typeString + "]);" +
		"enableTableShareAndPersistence(table=st2, tableName=`" + tbname + ", asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);" +
		"go\n" +
		"setStreamTableFilterColumn(" + tbname + ", `sym)")
	AssertNil(err)
	_, err = conn.RunScript(
		"n = 1000;table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL," + typeString + ", DOUBLE]);" +
			"table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, " + typeString + "]);" +
			"tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!([" + vecVal + "]),n),rand(100.00,n));" +
			"tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), take(array(" + typeString + ").append!([" + vecVal + "]),n));" +
			"d = dict(['msg1','msg2'], [table1, table2]);" +
			"replay(inputTables=d, outputTables=`" + tbname + ", dateColumn=`timestampv, timeColumn=`timestampv)")
	AssertNil(err)
	sdMap := make(map[string][2]string)
	sdMap["msg1"] = [2]string{"", "table1"}
	sdMap["msg2"] = [2]string{"", "table2"}
	opt := streaming.StreamDeserializerOption{
		TableNames: sdMap,
		Conn:       conn,
	}
	sd, err := streaming.NewStreamDeserializer(&opt)
	AssertNil(err)
	ex_types1 := []model.DataTypeByte{model.DtDatetime, model.DtTimestamp, model.DtSymbol, dataType + 64, model.DtDouble}
	args1 := make([]*model.Vector, 5)
	args1[0] = model.NewVector(model.NewDataTypeList(ex_types1[0], []model.DataType{}))
	args1[1] = model.NewVector(model.NewDataTypeList(ex_types1[1], []model.DataType{}))
	args1[2] = model.NewVector(model.NewDataTypeList(ex_types1[2], []model.DataType{}))
	args1[3] = model.NewVector(model.NewDataTypeList(ex_types1[3], []model.DataType{}))
	args1[4] = model.NewVector(model.NewDataTypeList(ex_types1[4], []model.DataType{}))
	ex_types2 := []model.DataTypeByte{model.DtDatetime, model.DtTimestamp, model.DtSymbol, dataType + 64}
	args2 := make([]*model.Vector, 4)
	args2[0] = model.NewVector(model.NewDataTypeList(ex_types2[0], []model.DataType{}))
	args2[1] = model.NewVector(model.NewDataTypeList(ex_types2[1], []model.DataType{}))
	args2[2] = model.NewVector(model.NewDataTypeList(ex_types2[2], []model.DataType{}))
	args2[3] = model.NewVector(model.NewDataTypeList(ex_types2[3], []model.DataType{}))

	var lock1 sync.Mutex
	var lock2 sync.Mutex
	plock1 := &lock1
	plock2 := &lock2
	sh := sdHandler_av{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock1}
	sbh := sdBatchHandler_av{*sd, 0, 0, args1, args2, ex_types1, ex_types2, plock2}
	fmt.Println("create handler successfully.")
	return sh, sbh
}

func getRandomClusterAddress() string {
	addressV := setup.HA_sites
	n, err := rand.Int(rand.Reader, big.NewInt(int64(len(addressV))))
	if err != nil {
		panic(err)
	}
	return addressV[n.Int64()]
}
