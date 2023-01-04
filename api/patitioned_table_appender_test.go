package api

import (
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestPartitionedTableAppender(t *testing.T) {
	opt := &PoolOption{
		Address:  testAddress,
		UserID:   "user",
		Password: "password",
		PoolSize: 2,
	}

	pool, err := NewDBConnectionPool(opt)
	assert.Nil(t, err)
	assert.Equal(t, pool.GetPoolSize(), 2)

	appenderOpt := &PartitionedTableAppenderOption{
		Pool:         pool,
		DBPath:       "dfs://test",
		TableName:    "pt",
		PartitionCol: "sym",
	}

	appender, err := NewPartitionedTableAppender(appenderOpt)
	assert.Nil(t, err)

	col, err := model.NewDataTypeListFromRawData(model.DtDate, []time.Time{time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC),
		time.Date(2022, time.Month(1), 1, 2, 1, 0, 0, time.UTC), time.Date(2022, time.Month(1), 1, 3, 1, 0, 0, time.UTC)})
	assert.Nil(t, err)

	col1, err := model.NewDataTypeListFromRawData(model.DtString, []string{"col1", "col1", "col1"})
	assert.Nil(t, err)

	tb := model.NewTable([]string{"sym"}, []*model.Vector{model.NewVector(col1)})
	n, err := appender.Append(tb)
	assert.Equal(t, err.Error(), "the input table doesn't match the schema of the target table")
	assert.Equal(t, n, 0)

	tb = model.NewTable([]string{"date", "sym"}, []*model.Vector{model.NewVector(col), model.NewVector(col1)})
	n, err = appender.Append(tb)
	assert.Nil(t, err)
	assert.Equal(t, n, 1)

	appenderOpt = &PartitionedTableAppenderOption{
		Pool:         pool,
		TableName:    "pt",
		PartitionCol: "sym",
	}

	appender, err = NewPartitionedTableAppender(appenderOpt)
	assert.Nil(t, err)

	assert.Nil(t, appender.Close())
}
