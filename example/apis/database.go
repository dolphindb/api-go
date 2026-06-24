package apis

import (
	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/logging"
)

// Database checks whether the Database api is valid.
func Database(db api.DolphinDB) (*api.Database, error) {
	d := new(api.DatabaseRequest).
		SetDirectory(dbPath).
		SetDBHandle(dbName)
	dt, err := db.Database(d)
	logging.Info("example.apis", "create database")
	return dt, err
}

// SegmentDatabase checks whether the Database api is valid when db is dfs db.
func SegmentDatabase(db api.DolphinDB) (*api.Database, error) {
	d := new(api.DatabaseRequest).
		SetDirectory(segmentDBPath).
		SetPartitionType("VALUE").
		SetPartitionScheme("1..10").
		SetLocations("").
		SetEngine("").
		SetAtomic("").
		SetDBHandle(dbName)
	dt, err := db.Database(d)
	logging.Info("example.apis", "create segment database")
	return dt, err
}

// DropSegmentDatabase checks whether the DropDatabase api is valid when db is dfs db.
func DropSegmentDatabase(db api.DolphinDB) error {
	d := new(api.DropDatabaseRequest).
		SetDirectory(segmentDBPath)
	err := db.DropDatabase(d)
	logging.Info("example.apis", "drop segment database")
	return err
}

// DropDatabase checks whether the DropDatabase api is valid.
func DropDatabase(db api.DolphinDB) error {
	d := new(api.DropDatabaseRequest).
		SetDirectory(dbPath)
	err := db.DropDatabase(d)
	logging.Info("example.apis", "drop database")
	return err
}

// ExistsDatabase checks whether the ExistsDatabase api is valid.
func ExistsDatabase(db api.DolphinDB) error {
	d := new(api.ExistsDatabaseRequest).
		SetPath(dbPath)
	b, err := db.ExistsDatabase(d)
	logging.Info("example.apis", "exists database", "exists", b)
	return err
}
