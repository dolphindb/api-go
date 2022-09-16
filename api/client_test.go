package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	_, err := NewDolphinDBClient(context.TODO(), testAddress, nil)
	assert.Nil(t, err)

	db, err := NewSimpleDolphinDBClient(context.TODO(), testAddress, "user", "password")
	assert.Nil(t, err)

	err = db.Connect()
	assert.Nil(t, err)

	existsDatabaseReq := new(ExistsDatabaseRequest).
		SetPath("dfs://db")
	b, err := db.ExistsDatabase(existsDatabaseReq)
	assert.Nil(t, err)
	assert.Equal(t, b, false)

	createDBReq := new(DatabaseRequest).
		SetDirectory("/db").
		SetPartitionType("VALUE").
		SetPartitionScheme("1..10").
		SetLocations("").
		SetEngine("").
		SetAtomic("").
		SetDBHandle("db")
	d, err := db.Database(createDBReq)
	assert.Nil(t, err)
	assert.Equal(t, d.Name, "db")

	dropReq := new(DropDatabaseRequest).
		SetDirectory("/db")
	err = db.DropDatabase(dropReq)
	assert.Nil(t, err)

	existTableReq := new(ExistsTableRequest).
		SetDBPath("/db1").
		SetTableName("test")
	b, err = db.ExistsTable(existTableReq)
	assert.Nil(t, err)
	assert.Equal(t, b, false)

	saveTableReq := new(SaveTableRequest).
		SetTable("test")
	err = db.SaveTable(saveTableReq)
	assert.Nil(t, err)

	loadTextReq := new(LoadTextRequest).
		SetFileName("/stock.csv")
	tb, err := db.LoadText(loadTextReq)
	assert.Nil(t, err)
	assert.Equal(t, tb.GetSession(), "20267359")

	saveTextReq := new(SaveTextRequest).
		SetFileName("/stock.csv").
		SetObj("test")
	err = db.SaveText(saveTextReq)
	assert.Nil(t, err)

	ploadReq := new(PloadTextRequest).
		SetFileName("/stock.csv")
	tb, err = db.PloadText(ploadReq)
	assert.Nil(t, err)
	assert.Equal(t, tb.GetSession(), "20267359")

	loadTableReq := new(LoadTableRequest).
		SetTableName("test").
		SetDatabase("/db")
	tb, err = db.LoadTable(loadTableReq)
	assert.Nil(t, err)
	assert.Equal(t, tb.GetSession(), "20267359")

	loadBySQLReq := new(LoadTableBySQLRequest).
		SetSQL("sql()").
		SetDBPath("dfs://db").
		SetTableName("test")
	tb, err = db.LoadTableBySQL(loadBySQLReq)
	assert.Nil(t, err)
	assert.Equal(t, tb.GetSession(), "20267359")

	tbCapReq := new(TableWithCapacityRequest).
		SetTableName("test").SetCapacity(100).SetSize(3).
		SetColNames([]string{"name", "id", "value"}).
		SetColTypes([]string{"string", "INT", "DOUBLE"})
	tb, err = db.TableWithCapacity(tbCapReq)
	assert.Nil(t, err)
	assert.Equal(t, tb.GetSession(), "20267359")

	tbReq := new(TableRequest).
		SetTableName("test").
		AddTableParam("id", "`XOM`GS`AAPL").
		AddTableParam("x", "102.1 33.4 73.6")
	tb, err = db.Table(tbReq)
	assert.Nil(t, err)
	assert.Equal(t, tb.GetSession(), "20267359")

	dropTableReq := new(DropTableRequest).
		SetTableName("test").
		SetDBHandle("db")
	err = db.DropTable(dropTableReq)
	assert.Nil(t, err)

	dropPartitionReq := new(DropPartitionRequest).
		SetPartitionPaths("GS").
		SetTableName("test").
		SetDBHandle("db")
	err = db.DropPartition(dropPartitionReq)
	assert.Nil(t, err)

	undefReq := new(UndefRequest).
		SetObj("`valu").
		SetObjType("INT")
	err = db.Undef(undefReq)
	assert.Nil(t, err)

	c := new(ClearAllCacheRequest).
		SetIsDFS(true)
	err = db.ClearAllCache(c)
	assert.Nil(t, err)

	err = db.UndefAll()
	assert.Nil(t, err)
}
