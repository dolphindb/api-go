package api

import (
	"bytes"
	"fmt"
	"strings"

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

	by := strings.Builder{}
	by.WriteString(handle)
	by.WriteString("=")
	by.WriteString(c.Name)
	by.WriteString(".createTable(")
	by.WriteString(t.SrcTable)
	by.WriteString(",`")
	by.WriteString(t.DimensionTableName)

	if len(t.SortColumns) > 0 {
		by.WriteString(",sortColumns=`")
		by.WriteString(strings.Join(t.SortColumns, "`"))
	}

	by.WriteString(")")

	_, err := c.db.RunScript(by.String())
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
	by := new(bytes.Buffer)

	by.WriteString(fmt.Sprintf("%s=%s.createPartitionedTable(%s, `%s, `%s", handle, c.Name,
		p.SrcTable, p.PartitionedTableName, strings.Join(p.PartitionColumns, "`")))

	if len(p.CompressMethods) > 0 {
		by.WriteString(",compressMethods={")
		for k, v := range p.CompressMethods {
			by.WriteString(fmt.Sprintf(`%s:"%s",`, k, v))
		}
		by.Truncate(by.Len() - 1)
	}

	if len(p.SortColumns) > 0 {
		by.WriteString(fmt.Sprintf(",sortColumns=`%s", strings.Join(p.SortColumns, "`")))
	}

	if len(p.KeepDuplicates) > 0 {
		by.WriteString(",keepDuplicates=" + p.KeepDuplicates)
	}
	by.WriteString(")")

	_, err := c.db.RunScript(by.String())
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
