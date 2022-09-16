package test

import (
	"context"
	"fmt"
	"reflect"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
)

const (
	DfsDBPath    = "dfs://test_dfsTable"
	TbName1      = "tb1"
	TbName2      = "tb2"
	DfsTBName1   = "pt1"
	DfsTBName2   = "pt2"
	DiskDBPath   = setup.WORKDIR + `/testTable`
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

func CreateDfsDimensiondb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsRangedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsRangedbChunkGranularity(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsHashdb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsHashdbChunkGranularity(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsValuedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsValuedbChunkGranularity(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsListdb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsListdbChunkGranularity(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsCompoRangeRangedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsCompoRangeRangedbChunkGranularity(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsCompoRangeValuedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsCompoRangeHashdb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDfsCompoRangeListdb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDiskUnpartitioneddb(dbPath string, tbName1 string, tbName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDiskRangedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDiskHashdb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDiskValuedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDiskListdb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDiskCompoRangeRangedb(dbPath string, tableName1 string, tableName2 string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func CreateDBConnectionPool(threadNumCount int, loadbalance bool) *api.DBConnectionPool {
	opt := &api.PoolOption{
		Address:     setup.Address,
		UserID:      setup.UserName,
		Password:    setup.Password,
		PoolSize:    threadNumCount,
		LoadBalance: loadbalance,
	}
	pool, err := api.NewDBConnectionPool(opt)
	AssertNil(err)
	return pool
}

func ClearEnv() {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
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

func ClearStreamTable(tableName string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
	AssertNil(err)
	script := "login(`admin,`123456);" +
		"try{dropStreamTable('" + tableName + "')}catch(ex){};"
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
