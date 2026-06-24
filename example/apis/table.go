package apis

import (
	"fmt"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/logging"
)

// ExistsTable checks whether the ExistsTable api is valid.
func ExistsTable(db api.DolphinDB) error {
	l := new(api.ExistsTableRequest).
		SetDBPath(dbPath).
		SetTableName(tableName)
	b, err := db.ExistsTable(l)
	logging.Info("example.apis", "exists table", "exists", b)
	return err
}

// SaveTable checks whether the SaveTable api is valid.
func SaveTable(db api.DolphinDB) error {
	l := new(api.SaveTableRequest).
		SetDBHandle(dbName).
		SetTable(tableName)
	err := db.SaveTable(l)
	logging.Info("example.apis", "save table")
	return err
}

// TableWithCapacity checks whether the TableWithCapacity api is valid.
func TableWithCapacity(db api.DolphinDB) (*api.Table, error) {
	l := new(api.TableWithCapacityRequest).
		SetTableName(tableName).SetCapacity(100).SetSize(3).
		SetColNames([]string{"name", "id", "value"}).
		SetColTypes([]string{"string", "INT", "DOUBLE"})
	t, err := db.TableWithCapacity(l)
	logging.Info("example.apis", "table with capacity", "table", t.String())
	return t, err
}

// Table checks whether the Table api is valid.
func Table(db api.DolphinDB) (*api.Table, error) {
	l := new(api.TableRequest).
		SetTableName(tableName).
		AddTableParam("id", "`XOM`GS`AAPL").
		AddTableParam("x", "102.1 33.4 73.6")
	t, err := db.Table(l)
	logging.Info("example.apis", "table", "table", t.String())
	return t, err
}

// DropTable checks whether the DropTable api is valid.
func DropTable(db api.DolphinDB) error {
	t := new(api.DropTableRequest).
		SetTableName(tableName).
		SetDBHandle(dbName)
	err := db.DropTable(t)
	logging.Info("example.apis", "drop table")
	return err
}

// DropSegmentTable checks whether the DropTable api is valid when drops dfs table.
func DropSegmentTable(db api.DolphinDB) error {
	t := new(api.DropTableRequest).
		SetTableName(segmentTableName).
		SetDBHandle(dbName)
	err := db.DropTable(t)
	logging.Info("example.apis", "drop segment table")
	return err
}

// DropPartitionTable checks whether the DropTable api is valid when drops dfs table.
func DropPartitionTable(db api.DolphinDB) error {
	t := new(api.DropTableRequest).
		SetTableName(partitionedTableName).
		SetDBHandle(dbName)
	err := db.DropTable(t)
	logging.Info("example.apis", "drop partition table")
	return err
}

// LoadTable checks whether the LoadTable api is valid.
func LoadTable(db api.DolphinDB) error {
	t := new(api.LoadTableRequest).
		SetTableName(tableName).
		SetDatabase(dbPath)
	df, err := db.LoadTable(t)
	logging.Info("example.apis", "load table", "table", df.String())
	return err
}

// LoadTableBySQL checks whether the LoadTableBySQL api is valid.
func LoadTableBySQL(db api.DolphinDB, na string) error {
	t := new(api.LoadTableBySQLRequest).
		SetSQL(fmt.Sprintf(loadSQL, na)).
		SetDBPath(segmentDBPath).
		SetTableName(partitionedTableName)
	df, err := db.LoadTableBySQL(t)
	logging.Info("example.apis", "load table by sql", "table", df.String())
	return err
}

// LoadText checks whether the LoadText api is valid.
func LoadText(db api.DolphinDB) error {
	t := new(api.LoadTextRequest).
		SetFileName(remoteFilePath)
	di, err := db.LoadText(t)
	logging.Info("example.apis", "load text", "table", di.String())
	return err
}

// PloadText checks whether the PloadText api is valid.
func PloadText(db api.DolphinDB) error {
	t := new(api.PloadTextRequest).
		SetFileName(remoteFilePath)
	di, err := db.PloadText(t)
	logging.Info("example.apis", "pload text", "table", di.String())
	return err
}

// SaveText checks whether the SaveText api is valid.
func SaveText(db api.DolphinDB) error {
	t := new(api.SaveTextRequest).
		SetFileName(remoteFilePath).
		SetObj(tableName)
	err := db.SaveText(t)
	logging.Info("example.apis", "save text")
	return err
}

// CreateTable checks whether the CreateTable api is valid.
func CreateTable(db *api.Database) (*api.Table, error) {
	c := new(api.CreateTableRequest).
		SetSrcTable(tableName).
		SetDimensionTableName(segmentTableName)
	t, err := db.CreateTable(c)
	logging.Info("example.apis", "create table", "table", t.String())

	return t, err
}

// CreatePartitionedTable checks whether the CreatePartitionedTable api is valid.
func CreatePartitionedTable(db *api.Database) (*api.Table, error) {
	c := new(api.CreatePartitionedTableRequest).
		SetSrcTable(tableName).
		SetPartitionedTableName(partitionedTableName).
		SetPartitionColumns([]string{"id"})
	t, err := db.CreatePartitionedTable(c)
	logging.Info("example.apis", "create partitioned table", "table", t.String())

	return t, err
}

// DropPartition checks whether the DropPartition api is valid.
func DropPartition(db api.DolphinDB) error {
	t := new(api.DropPartitionRequest).
		SetPartitionPaths("GS").
		SetTableName(partitionedTableName).
		SetDBHandle(dbName)
	err := db.DropPartition(t)
	logging.Info("example.apis", "drop partition")
	return err
}

// LoadPartitionedTable checks whether the LoadTable api is valid when table is dfs table.
func LoadPartitionedTable(db api.DolphinDB) error {
	t := new(api.LoadTableRequest).
		SetTableName(partitionedTableName).
		SetDatabase(segmentDBPath)
	df, err := db.LoadTable(t)
	logging.Info("example.apis", "load partitioned table", "table", df.String())
	return err
}
