package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
)

// DolphinDB interface declares functions to communicate with the dolphindb server.
type DolphinDB interface {
	dialer.Conn

	AccountAPI
	DatabaseAPI
	TableAPI
}

type dolphindb struct {
	dialer.Conn

	addr string

	ctx context.Context
}

// TableAPI interface declares apis about table.
type TableAPI interface {
	// ExistsTable checks whether the table is existed.
	// See DolphinDB function `existsTable`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/e/existsTable.html?highlight=existstable
	ExistsTable(e *ExistsTableRequest) (bool, error)
	// Table creates an in-memory table with columns.
	// See DolphinDB function `table`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/t/table.html?highlight=table
	Table(t *TableRequest) (*Table, error)
	// TableWithCapacity creates an in-memory table with a specific capacity.
	// See DolphinDB function `table`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/t/table.html?highlight=table
	TableWithCapacity(t *TableWithCapacityRequest) (*Table, error)
	// SaveTable saves a table.
	// See DolphinDB function `saveTable`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/s/saveTable.html?highlight=savetable
	SaveTable(s *SaveTableRequest) error
	// LoadTable loads a table into memory.
	// See DolphinDB function `loadTable`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/l/loadTable.html?highlight=loadtable
	LoadTable(l *LoadTableRequest) (*Table, error)
	// LoadText loads text from a file.
	// See DolphinDB function `loadText`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/l/loadText.html?highlight=loadtext
	LoadText(l *LoadTextRequest) (*Table, error)
	// SaveText saves text into a file.
	// See DolphinDB function `saveText`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/s/saveText.html?highlight=savetext
	SaveText(l *SaveTextRequest) error
	// PloadText loads text from a file.
	// See DolphinDB function `pLoadText`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/p/ploadText.html?highlight=ploadtext
	PloadText(l *PloadTextRequest) (*Table, error)
	// LoadTableBySQL loads a table using a SQL query.
	// See DolphinDB function `loadTableBySQL`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/l/loadTableBySQL.html?highlight=loadtablebysql
	LoadTableBySQL(l *LoadTableBySQLRequest) (*Table, error)
	// DropPartition drops the specified partition from a database.
	// See DolphinDB function `dropPartition`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/d/dropPartition.html?highlight=droppartition
	DropPartition(l *DropPartitionRequest) error
	// DropTable drops a table.
	// See DolphinDB function `dropTable`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/d/dropTable.html?highlight=droptable
	DropTable(d *DropTableRequest) error
	// Undef releases the specified objects
	// See DolphinDB function `undef`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/u/undef.html?highlight=unde
	Undef(u *UndefRequest) error
	// UndefAll releases all objects
	// See DolphinDB function `undefAll`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/u/undef.html?highlight=unde
	UndefAll() error
	// ClearAllCache clears all cache.
	// See DolphinDB function `clearAllCache`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/c/clearAllCache.html?highlight=clearallcache
	ClearAllCache(r *ClearAllCacheRequest) error
}

// DatabaseAPI interface declares apis about database.
type DatabaseAPI interface {
	// ExistsDatabase checks whether the database already exists.
	// See DolphinDB function `existsDatabase`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/e/existsDatabase.html?highlight=existsdatabase
	ExistsDatabase(e *ExistsDatabaseRequest) (bool, error)

	// Database creates a database
	// See DolphinDB function `database`：https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/FunctionReferences/d/database.html?highlight=database
	Database(d *DatabaseRequest) (*Database, error)

	// DropDatabase drops a database.
	// See DolphinDB function `dropDatabase`: https://www.dolphindb.cn/cn/help/130/FunctionsandCommands/CommandsReferences/d/dropDatabase.html?highlight=dropdatabase
	DropDatabase(d *DropDatabaseRequest) error
}

// NewDolphinDBClient returns an instance of DolphinDB according to the addr and
// the flags which will affect the subsequent api calls.
func NewDolphinDBClient(ctx context.Context, addr string, flags *dialer.BehaviorOptions) (DolphinDB, error) {
	var err error

	c := &dolphindb{
		ctx:  ctx,
		addr: addr,
	}

	c.Conn, err = dialer.NewConn(ctx, c.addr, flags)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// NewSimpleDolphinDBClient returns an instance of DolphinDB which has logged in.
func NewSimpleDolphinDBClient(ctx context.Context, addr, userID, pwd string) (DolphinDB, error) {
	var err error

	c := &dolphindb{
		ctx:  ctx,
		addr: addr,
	}

	c.Conn, err = dialer.NewSimpleConn(ctx, addr, userID, pwd)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *dolphindb) ExistsDatabase(e *ExistsDatabaseRequest) (bool, error) {
	res, err := c.RunScript(fmt.Sprintf("existsDatabase('%s')", e.Path))
	if err != nil {
		return false, err
	}

	return res.(*model.Scalar).Bool()
}

func (c *dolphindb) Database(d *DatabaseRequest) (*Database, error) {
	cmd := generateCreateDatabaseParam(d)
	if d.DBHandle == "" {
		d.DBHandle = generateDBName()
	}

	_, err := c.RunScript(fmt.Sprintf("%s=database(%s)", d.DBHandle, cmd))
	if err != nil {
		return nil, err
	}

	return &Database{
		db:   c,
		Name: d.DBHandle,
	}, nil
}

func (c *dolphindb) DropDatabase(d *DropDatabaseRequest) error {
	_, err := c.RunScript(fmt.Sprintf("dropDatabase('%s')", d.Directory))
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) ExistsTable(t *ExistsTableRequest) (bool, error) {
	res, err := c.RunScript(fmt.Sprintf("existsTable('%s','%s')", t.DBPath, t.TableName))
	if err != nil {
		return false, err
	}

	return res.(*model.Scalar).Bool()
}

func (c *dolphindb) SaveTable(t *SaveTableRequest) error {
	if t.DBHandle == "" {
		t.DBHandle = generateDBName()
		_, err := c.RunScript(fmt.Sprintf("%s=database('%s')", t.DBHandle, t.DBPath))
		if err != nil {
			return err
		}
	}
	_, err := c.RunScript(fmt.Sprintf("saveTable(%s)", generateSaveTableParam(t)))
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) LoadText(l *LoadTextRequest) (*Table, error) {
	if l.Delimiter == "" {
		l.Delimiter = ","
	}
	handle := generateTableName()
	_, err := c.RunScript(fmt.Sprintf(`%s=loadText("%s","%s")`, handle, l.FileName, l.Delimiter))
	if err != nil {
		return nil, err
	}

	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, handle))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c,
		Data:   df.(*model.Table),
		Handle: handle,
	}, nil
}

// func (c *dolphindb) LoadTextEx(l *LoadTextExRequest) (*Table, error) {
// 	if l.Delimiter == "" {
// 		l.Delimiter = ","
// 	}

// 	handle := generateTableName()

// 	by := new(bytes.Buffer)
// 	by.WriteString(fmt.Sprintf(`%s=loadTextEx(%s,"%s"`, handle, l.DBName, handle))
// 	if len(l.PartitionColumns) != 0 {
// 		by.WriteString(fmt.Sprintf(",`%s,\"%s\",%s", strings.Join(l.PartitionColumns, "`"), l.RemoteFilePath, l.Delimiter))
// 	} else {
// 		by.WriteString(fmt.Sprintf(`, ,"%s", %s)`, l.RemoteFilePath, l.Delimiter))
// 	}
// 	_, err := c.RunScript(by.String())
// 	if err != nil {
// 		return nil, err
// 	}

// 	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, handle))
// 	if err != nil {
// 		return nil, err
// 	}

// 	return new(Table).
// 		setDB(c).
// 		SetHandle(handle).
// 		setData(df.(*model.Table)), nil
// }

func (c *dolphindb) SaveText(l *SaveTextRequest) error {
	_, err := c.RunScript(fmt.Sprintf(`saveText(%s,"%s")`, l.Obj, l.FileName))
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) PloadText(p *PloadTextRequest) (*Table, error) {
	if p.Delimiter == "" {
		p.Delimiter = ","
	}
	handle := generateTableName()

	_, err := c.RunScript(fmt.Sprintf(`%s=ploadText("%s","%s")`, handle, p.FileName, p.Delimiter))
	if err != nil {
		return nil, err
	}

	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, handle))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c,
		Data:   df.(*model.Table),
		Handle: handle,
	}, nil
}

func (c *dolphindb) LoadTable(l *LoadTableRequest) (*Table, error) {
	handle := generateTableName()
	_, err := c.RunScript(fmt.Sprintf(`%s=loadTable("%s","%s",%s,%t)`, handle, l.Database, l.TableName, l.Partitions, l.MemoryMode))
	if err != nil {
		return nil, err
	}

	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, handle))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c,
		Data:   df.(*model.Table),
		Handle: handle,
	}, nil
}

func (c *dolphindb) LoadTableBySQL(l *LoadTableBySQLRequest) (*Table, error) {
	if l.DBHandle == "" {
		l.DBHandle = generateDBName()
		_, err := c.RunScript(fmt.Sprintf(`%s=database("%s")`, l.DBHandle, l.DBPath))
		if err != nil {
			return nil, err
		}
	}

	_, err := c.RunScript(fmt.Sprintf(`%s=%s.loadTable("%s")`, l.TableName, l.DBHandle, l.TableName))
	if err != nil {
		return nil, err
	}

	var sql string
	if strings.HasPrefix(l.SQL, "sql(") {
		_, err := c.RunScript(fmt.Sprintf(`st=%s`, l.SQL))
		if err != nil {
			return nil, err
		}

		sql = "st"
	} else {
		sql = "< " + l.SQL + " >"
	}

	handle := generateTableName()
	_, err = c.RunScript(fmt.Sprintf(`%s=loadTableBySQL(%s)`, handle, sql))
	if err != nil {
		return nil, err
	}

	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, handle))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c,
		Data:   df.(*model.Table),
		Handle: handle,
	}, nil
}

func (c *dolphindb) TableWithCapacity(t *TableWithCapacityRequest) (*Table, error) {
	_, err := c.RunScript(fmt.Sprintf("%s=table(%d:%d, `%s, [%s])", t.TableName, t.Capacity,
		t.Size, strings.Join(t.ColNames, "`"), strings.Join(t.ColTypes, ",")))
	if err != nil {
		return nil, err
	}

	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, t.TableName))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c,
		Data:   df.(*model.Table),
		Handle: t.TableName,
	}, nil
}

func (c *dolphindb) Table(t *TableRequest) (*Table, error) {
	names := make([]string, len(t.TableParams))
	for k, v := range t.TableParams {
		names[k] = v.Key
		_, err := c.RunScript(fmt.Sprintf("%s=%s", v.Key, v.Value))
		if err != nil {
			return nil, err
		}
	}
	_, err := c.RunScript(fmt.Sprintf("%s=table(%v)", t.TableName, strings.Join(names, ", ")))
	if err != nil {
		return nil, err
	}

	df, err := c.RunScript(fmt.Sprintf(`select * from %s`, t.TableName))
	if err != nil {
		return nil, err
	}

	return &Table{
		db:     c,
		Data:   df.(*model.Table),
		Handle: t.TableName,
	}, nil
}

func (c *dolphindb) DropTable(d *DropTableRequest) error {
	if d.DBHandle == "" {
		d.DBHandle = generateDBName()
		_, err := c.RunScript(fmt.Sprintf(`%s=database("%s")`, d.DBHandle, d.DBPath))
		if err != nil {
			return err
		}
	}

	_, err := c.RunScript(fmt.Sprintf("dropTable(%s,'%s')", d.DBHandle, d.TableName))
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) DropPartition(d *DropPartitionRequest) error {
	if d.DBHandle == "" {
		d.DBHandle = generateDBName()
		_, err := c.RunScript(fmt.Sprintf(`%s=database("%s")`, d.DBHandle, d.DBPath))
		if err != nil {
			return err
		}
	}

	_, err := c.RunScript(fmt.Sprintf("dropPartition(%s, %s, tableName=`%s)", d.DBHandle, d.PartitionPaths, d.TableName))
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) Undef(u *UndefRequest) error {
	s := u.Obj
	if u.ObjType != "" {
		s += "," + u.ObjType
	}

	_, err := c.RunScript(fmt.Sprintf("undef(%s)", s))
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) UndefAll() error {
	_, err := c.RunScript("undef all")
	if err != nil {
		return err
	}

	return nil
}

func (c *dolphindb) ClearAllCache(r *ClearAllCacheRequest) error {
	var err error
	if r.IsDFS {
		_, err = c.RunScript("pnodeRun(clearAllCache)")
	} else {
		_, err = c.RunScript("clearAllCache()")
	}
	if err != nil {
		return err
	}

	return nil
}
