package api

import (
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/model"

	"github.com/stretchr/testify/assert"
)

func TestNewPartitionedTableAppenderValidatesOption(t *testing.T) {
	_, err := NewPartitionedTableAppender(nil)
	assert.EqualError(t, err, "partitioned table appender option must not be nil")

	_, err = NewPartitionedTableAppender(&PartitionedTableAppenderOption{})
	assert.EqualError(t, err, "partitioned table appender connection pool must not be nil")

	_, err = NewPartitionedTableAppender(&PartitionedTableAppenderOption{Pool: &DBConnectionPool{}})
	assert.EqualError(t, err, "partitioned table appender table name must not be empty")

	_, err = NewPartitionedTableAppender(&PartitionedTableAppenderOption{
		Pool:      &DBConnectionPool{},
		TableName: "pt",
	})
	assert.EqualError(t, err, "partitioned table appender partition column must not be empty")
}

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

	tb := mustNewTable(t, []string{"sym"}, []*model.Vector{model.NewVector(col1)})
	n, err := appender.Append(tb)
	assert.Equal(t, err.Error(), "the input table doesn't match the schema of the target table")
	assert.Equal(t, n, 0)

	tb = mustNewTable(t, []string{"date", "sym"}, []*model.Vector{model.NewVector(col), model.NewVector(col1)})
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
