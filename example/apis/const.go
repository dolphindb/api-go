package apis

const (
	// User is the Dolphindb userID.
	User = "admin"
	// Password is password of the user.
	Password = "123456"
	// TestAddr is the Dolphindb server address.
	TestAddr = "127.0.0.1:8848"

	dbPath               = "/tmp/db"
	segmentDBPath        = "dfs://db"
	tableName            = "test"
	segmentTableName     = "segment"
	partitionedTableName = "partitioned"
	dbName               = "db"
	remoteFilePath       = "/home/zcwen/stock.csv"
	loadSQL              = "select name,id,value from %s"
)
