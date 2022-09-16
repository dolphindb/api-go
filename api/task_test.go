package api

import (
	"testing"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestTask(t *testing.T) {
	dt, err := model.NewDataType(model.DtString, "task")
	assert.Nil(t, err)

	s := model.NewScalar(dt)
	task := &Task{
		result: s,
		err:    nil,
	}
	res := task.GetResult()
	assert.Equal(t, res.String(), "string(task)")
	assert.Nil(t, task.GetError())
	assert.Equal(t, task.IsSuccess(), true)
}
