package api

import "github.com/dolphindb/api-go/model"

// Task is the unit of work that is executed in the DBConnectionPool.
type Task struct {
	// Script is required
	Script string
	// Args is optional, if you set it, the task will be executed by RunFunc or by RunScript
	Args []model.DataForm

	result model.DataForm
	err    error
}

// GetResult returns the execution result of the task.
func (t *Task) GetResult() model.DataForm {
	return t.result
}

// IsSuccess checks whether the task is executed successfully.
func (t *Task) IsSuccess() bool {
	return t.err == nil
}

// GetError gets the execution error of the task.
func (t *Task) GetError() error {
	return t.err
}
