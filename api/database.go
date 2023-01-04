package api

import (
	"fmt"

	"github.com/dolphindb/api-go/model"
)

// Database is used to call table api based on the name of db.
type Database struct {
	db *dolphindb

	Name string
}

// GetSession returns the sessionID of the session.
func (c *Database) GetSession() string {
	return c.db.GetSession()
}

// CreateTable creates an in-memory table in the database and returns the table instance.
func (c *Database) CreateTable(t *CreateTableRequest) (*Table, error) {
	handle := generateTableName()

	_, err := c.db.RunScript(generateCreateTable(handle, c.Name, t))
	if err != nil {
		return nil, err
	}

	df, err := c.db.RunScript(fmt.Sprintf(`select * from %s`, handle))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c.db,
		Data:   df.(*model.Table),
		Handle: handle,
	}, nil
}

// CreatePartitionedTable creates a partitioned table in the database and returns the table instance.
func (c *Database) CreatePartitionedTable(p *CreatePartitionedTableRequest) (*Table, error) {
	handle := generateTableName()

	_, err := c.db.RunScript(generateCreatePatitionedTable(handle, c.Name, p))
	if err != nil {
		return nil, err
	}

	df, err := c.db.RunScript(fmt.Sprintf(`select * from %s`, handle))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c.db,
		Data:   df.(*model.Table),
		Handle: handle,
	}, nil
}
