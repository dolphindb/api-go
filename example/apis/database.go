package apis

import (
	"fmt"

	"github.com/dolphindb/api-go/api"
)

// Database checks whether the Database api is valid.
func Database(db api.DolphinDB) (*api.Database, error) {
	d := new(api.DatabaseRequest).
		SetDirectory(dbPath).
		SetDBHandle(dbName)
	dt, err := db.Database(d)

	fmt.Println("CreateDatabase")
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

	fmt.Println("CreateSegmentDatabase")
	return dt, err
}

// DropSegmentDatabase checks whether the DropDatabase api is valid when db is dfs db.
func DropSegmentDatabase(db api.DolphinDB) error {
	d := new(api.DropDatabaseRequest).
		SetDirectory(segmentDBPath)
	err := db.DropDatabase(d)

	fmt.Println("DropSegmentDatabase")
	return err
}

// DropDatabase checks whether the DropDatabase api is valid.
func DropDatabase(db api.DolphinDB) error {
	d := new(api.DropDatabaseRequest).
		SetDirectory(dbPath)
	err := db.DropDatabase(d)

	fmt.Println("DropDatabase")
	return err
}

// ExistsDatabase checks whether the ExistsDatabase api is valid.
func ExistsDatabase(db api.DolphinDB) error {
	d := new(api.ExistsDatabaseRequest).
		SetPath(dbPath)
	b, err := db.ExistsDatabase(d)

	fmt.Println("ExistsDatabase", b)
	return err
}
