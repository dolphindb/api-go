package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabase(t *testing.T) {
	db, err := NewDolphinDBClient(context.TODO(), testAddress, nil)
	assert.Nil(t, err)

	err = db.Connect()
	assert.Nil(t, err)

	d := &Database{
		db: db.(*dolphindb),
	}

	cReq := new(CreateTableRequest).
		SetSrcTable("test").
		SetDimensionTableName("dst")
	tb, err := d.CreateTable(cReq)
	assert.Nil(t, err)
	assert.Equal(t, d.GetSession(), tb.GetSession())

	createPartitionReq := new(CreatePartitionedTableRequest).
		SetSrcTable("test").
		SetPartitionedTableName("partitioned").
		SetPartitionColumns([]string{"id"}).
		SetCompressMethods(map[string]string{"id": "delta"}).
		SetSortColumns([]string{"id"}).
		SetKeepDuplicates("true")
	tb, err = d.CreatePartitionedTable(createPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, d.GetSession(), tb.GetSession())
}
