package api

import (
	"testing"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	opt := &PoolOption{
		Address:     testAddress,
		UserID:      "user",
		Password:    "password",
		PoolSize:    2,
		LoadBalance: false,
	}

	pool, err := NewDBConnectionPool(opt)
	assert.Nil(t, err)
	assert.Equal(t, pool.GetPoolSize(), 2)

	dt, err := model.NewDataType(model.DtString, "test")
	assert.Nil(t, err)

	s := model.NewScalar(dt)
	task := &Task{
		Script: "typestr",
		Args:   []model.DataForm{s},
	}

	err = pool.Execute([]*Task{task, task, task})
	assert.Nil(t, err)

	err = pool.Execute([]*Task{task})
	assert.Nil(t, err)

	assert.Nil(t, task.GetError())
	assert.Equal(t, task.IsSuccess(), true)

	err = pool.Close()
	assert.Nil(t, err)

	opt.LoadBalance = true

	pool, err = NewDBConnectionPool(opt)
	assert.Nil(t, err)
	assert.Equal(t, pool.GetPoolSize(), 2)

	task1 := &Task{
		Script: "login",
	}

	err = pool.Execute([]*Task{task, task1})
	assert.Nil(t, err)

	assert.Nil(t, task.GetError())
	assert.Equal(t, task.IsSuccess(), true)

	assert.False(t, pool.IsClosed())
	err = pool.Close()
	assert.Nil(t, err)
	assert.True(t, pool.IsClosed())
}
