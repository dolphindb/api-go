package api

import (
	"github.com/dolphindb/api-go/model"
)

// Table is the client of table script.
// TODO: supports table script.
type Table struct {
	db *dolphindb

	// Handle is the handle of Table which has been defined on server
	Handle string
	// Data is the real value of Table
	Data *model.Table
}

// GetHandle returns the variable name of Table which has been defined on server.
func (t *Table) GetHandle() string {
	return t.Handle
}

// GetSession returns the session id of the connection to dolphindb.
func (t *Table) GetSession() string {
	return t.db.GetSession()
}

// String returns the string format of the data.
func (t *Table) String() string {
	return t.Data.String()
}
