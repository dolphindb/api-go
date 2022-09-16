package api

import (
	"context"
	"testing"
	"time"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
	"github.com/stretchr/testify/assert"
)

func TestTableAppender(t *testing.T) {
	conn, err := dialer.NewSimpleConn(context.TODO(), testAddress, "user", "password")
	assert.Nil(t, err)

	opt := &TableAppenderOption{
		DBPath:    "db",
		TableName: "table",
		Conn:      conn,
	}

	ta := NewTableAppender(opt)
	assert.NotNil(t, ta)

	col, err := model.NewDataTypeListWithRaw(model.DtTimestamp, []time.Time{time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC),
		time.Date(2022, time.Month(1), 1, 2, 1, 0, 0, time.UTC), time.Date(2022, time.Month(1), 1, 3, 1, 0, 0, time.UTC)})
	assert.Nil(t, err)

	col1, err := model.NewDataTypeListWithRaw(model.DtString, []string{"col1", "col1", "col1"})
	assert.Nil(t, err)

	tb := model.NewTable([]string{"date", "sym"}, []*model.Vector{model.NewVector(col), model.NewVector(col1)})
	res, err := ta.Append(tb)
	assert.Nil(t, err)
	assert.Equal(t, res.String(), "int(1)")

	assert.Equal(t, ta.IsClosed(), false)

	err = ta.Close()
	assert.Nil(t, err)
	assert.Equal(t, ta.IsClosed(), true)
}
