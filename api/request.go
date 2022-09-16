package api

// ExistsDatabaseRequest is the request struct of ExistsDatabase api.
type ExistsDatabaseRequest struct {
	Path string
}

// SetPath sets the Path value of ExistsDatabaseRequest.
func (e *ExistsDatabaseRequest) SetPath(path string) *ExistsDatabaseRequest {
	e.Path = path
	return e
}

// DatabaseRequest is the request struct of Database api.
type DatabaseRequest struct {
	DBHandle        string
	Directory       string
	PartitionType   string
	PartitionScheme string
	Locations       string
	Engine          string
	Atomic          string
}

// SetDBHandle sets the DBHandle value of DatabaseRequest.
func (d *DatabaseRequest) SetDBHandle(dbHandle string) *DatabaseRequest {
	d.DBHandle = dbHandle
	return d
}

// SetDirectory sets the Directory value of DatabaseRequest.
func (d *DatabaseRequest) SetDirectory(directory string) *DatabaseRequest {
	d.Directory = directory
	return d
}

// SetPartitionType sets the PartitionType value of DatabaseRequest.
func (d *DatabaseRequest) SetPartitionType(partitionType string) *DatabaseRequest {
	d.PartitionType = partitionType
	return d
}

// SetPartitionScheme sets the PartitionScheme value of DatabaseRequest.
func (d *DatabaseRequest) SetPartitionScheme(partitionScheme string) *DatabaseRequest {
	d.PartitionScheme = partitionScheme
	return d
}

// SetLocations sets the Locations value of DatabaseRequest.
func (d *DatabaseRequest) SetLocations(locations string) *DatabaseRequest {
	d.Locations = locations
	return d
}

// SetAtomic sets the Atomic value of DatabaseRequest.
func (d *DatabaseRequest) SetAtomic(atomic string) *DatabaseRequest {
	d.Atomic = atomic
	return d
}

// SetEngine sets the Engine value of DatabaseRequest.
func (d *DatabaseRequest) SetEngine(engine string) *DatabaseRequest {
	d.Engine = engine
	return d
}

// DropDatabaseRequest is the request struct of DropDatabase api.
type DropDatabaseRequest struct {
	Directory string
}

// SetDirectory sets the Directory value of DropDatabaseRequest.
func (d *DropDatabaseRequest) SetDirectory(directory string) *DropDatabaseRequest {
	d.Directory = directory
	return d
}

// ExistsTableRequest is the request struct of ExistsTable api.
type ExistsTableRequest struct {
	TableName string
	DBPath    string
}

// SetTableName sets the TableName value of ExistsTableRequest.
func (e *ExistsTableRequest) SetTableName(name string) *ExistsTableRequest {
	e.TableName = name
	return e
}

// SetDBPath sets the DBPath value of ExistsTableRequest.
func (e *ExistsTableRequest) SetDBPath(path string) *ExistsTableRequest {
	e.DBPath = path
	return e
}

// SaveTableRequest is the request struct of SaveTable api.
// If you have declared a DBHandle before, you can set it,
// or you should set the DBPath.
type SaveTableRequest struct {
	DBHandle    string
	DBPath      string
	TableName   string
	Table       string
	Appending   bool
	Compression bool
}

// SetDBHandle sets the DBHandle value of SaveTableRequest.
func (s *SaveTableRequest) SetDBHandle(name string) *SaveTableRequest {
	s.DBHandle = name
	return s
}

// SetDBPath sets the DBPath value of SaveTableRequest.
func (s *SaveTableRequest) SetDBPath(path string) *SaveTableRequest {
	s.DBPath = path
	return s
}

// SetTableName sets the TableName value of SaveTableRequest.
func (s *SaveTableRequest) SetTableName(name string) *SaveTableRequest {
	s.TableName = name
	return s
}

// SetTable sets the Table value of SaveTableRequest.
func (s *SaveTableRequest) SetTable(path string) *SaveTableRequest {
	s.Table = path
	return s
}

// SetAppending sets the Appending value of SaveTableRequest.
func (s *SaveTableRequest) SetAppending(appending bool) *SaveTableRequest {
	s.Appending = appending
	return s
}

// SetCompression  sets the Compression value of SaveTableRequest.
func (s *SaveTableRequest) SetCompression(compression bool) *SaveTableRequest {
	s.Compression = compression
	return s
}

// LoadTextRequest is the request struct of LoadText api.
type LoadTextRequest struct {
	FileName  string
	Delimiter string
}

// SetFileName sets the FileName value of LoadTextRequest.
func (l *LoadTextRequest) SetFileName(filename string) *LoadTextRequest {
	l.FileName = filename
	return l
}

// SetDelimiter sets the Delimiter value of LoadTextRequest.
func (l *LoadTextRequest) SetDelimiter(delimiter string) *LoadTextRequest {
	l.Delimiter = delimiter
	return l
}

// type LoadTextExRequest struct {
// 	DBName         string
// 	TableName      string
// 	RemoteFilePath string
// 	Delimiter      string

// 	PartitionColumns []string
// }

// func (l *LoadTextExRequest) SetDBName(name string) *LoadTextExRequest {
// 	l.DBName = name
// 	return l
// }

// func (l *LoadTextExRequest) SetTableName(name string) *LoadTextExRequest {
// 	l.TableName = name
// 	return l
// }

// func (l *LoadTextExRequest) SetPartitionColumns(cols []string) *LoadTextExRequest {
// 	l.PartitionColumns = cols
// 	return l
// }

// func (l *LoadTextExRequest) SetRemoteFilePath(remoteFilePath string) *LoadTextExRequest {
// 	l.RemoteFilePath = remoteFilePath
// 	return l
// }

// func (l *LoadTextExRequest) SetDelimiter(delimiter string) *LoadTextExRequest {
// 	l.Delimiter = delimiter
// 	return l
// }

// SaveTextRequest is the request struct of SaveText api.
type SaveTextRequest struct {
	Obj      string
	FileName string
	//	Delimiter string
}

// SetFileName sets the FileName value of SaveTextRequest.
func (l *SaveTextRequest) SetFileName(filename string) *SaveTextRequest {
	l.FileName = filename
	return l
}

// SetObj sets the Obj value of SaveTextRequest.
func (l *SaveTextRequest) SetObj(objName string) *SaveTextRequest {
	l.Obj = objName
	return l
}

// PloadTextRequest is the request struct of PloadText api.
type PloadTextRequest struct {
	FileName  string
	Delimiter string
}

// SetFileName sets the FileName value of PloadTextRequest.
func (p *PloadTextRequest) SetFileName(filename string) *PloadTextRequest {
	p.FileName = filename
	return p
}

// SetDelimiter sets the Delimiter value of PloadTextRequest.
func (p *PloadTextRequest) SetDelimiter(delimiter string) *PloadTextRequest {
	p.Delimiter = delimiter
	return p
}

// LoadTableRequest is the request struct of LoadTable api.
type LoadTableRequest struct {
	Database   string
	TableName  string
	MemoryMode bool

	Partitions string
}

// SetDatabase sets the Database value of LoadTableRequest.
func (l *LoadTableRequest) SetDatabase(database string) *LoadTableRequest {
	l.Database = database
	return l
}

// SetMemoryMode sets the MemoryMode value of LoadTableRequest.
func (l *LoadTableRequest) SetMemoryMode(memoryMode bool) *LoadTableRequest {
	l.MemoryMode = memoryMode
	return l
}

// SetTableName sets the TableName value of LoadTableRequest.
func (l *LoadTableRequest) SetTableName(name string) *LoadTableRequest {
	l.TableName = name
	return l
}

// SetPartitions sets the Partitions value of LoadTableRequest.
func (l *LoadTableRequest) SetPartitions(data string) *LoadTableRequest {
	l.Partitions = data
	return l
}

// LoadTableBySQLRequest is the request struct of LoadTableBySQL api.
// If you have declared a DBHandle before, you can set it,
// or you should set the DBPath.
type LoadTableBySQLRequest struct {
	DBPath    string
	TableName string
	SQL       string
	DBHandle  string
}

// SetDBHandle sets the DBHandle value of LoadTableBySQLRequest.
func (l *LoadTableBySQLRequest) SetDBHandle(dbHandle string) *LoadTableBySQLRequest {
	l.DBHandle = dbHandle
	return l
}

// SetDBPath sets the DBPath value of LoadTableBySQLRequest.
func (l *LoadTableBySQLRequest) SetDBPath(path string) *LoadTableBySQLRequest {
	l.DBPath = path
	return l
}

// SetTableName sets the TableName value of LoadTableBySQLRequest.
func (l *LoadTableBySQLRequest) SetTableName(tableName string) *LoadTableBySQLRequest {
	l.TableName = tableName
	return l
}

// SetSQL sets the SQL value of LoadTableBySQLRequest.
func (l *LoadTableBySQLRequest) SetSQL(sql string) *LoadTableBySQLRequest {
	l.SQL = sql
	return l
}

// CreatePartitionedTableRequest is the request struct of CreatePartitionedTable api.
type CreatePartitionedTableRequest struct {
	SrcTable             string
	PartitionedTableName string
	PartitionColumns     []string
	CompressMethods      map[string]string
	SortColumns          []string
	KeepDuplicates       string
}

// SetCompressMethods sets the CompressMethods value of CreatePartitionedTableRequest.
func (c *CreatePartitionedTableRequest) SetCompressMethods(compressMethods map[string]string) *CreatePartitionedTableRequest {
	c.CompressMethods = compressMethods
	return c
}

// SetSortColumns sets the SortColumns value of CreatePartitionedTableRequest.
func (c *CreatePartitionedTableRequest) SetSortColumns(sortColumns []string) *CreatePartitionedTableRequest {
	c.SortColumns = sortColumns
	return c
}

// SetKeepDuplicates sets the KeepDuplicates value of CreatePartitionedTableRequest.
func (c *CreatePartitionedTableRequest) SetKeepDuplicates(keepDuplicates string) *CreatePartitionedTableRequest {
	c.KeepDuplicates = keepDuplicates
	return c
}

// SetSrcTable sets the SrcTable value of CreatePartitionedTableRequest.
func (c *CreatePartitionedTableRequest) SetSrcTable(name string) *CreatePartitionedTableRequest {
	c.SrcTable = name
	return c
}

// SetPartitionedTableName sets the PartitionedTableName value of CreatePartitionedTableRequest.
func (c *CreatePartitionedTableRequest) SetPartitionedTableName(name string) *CreatePartitionedTableRequest {
	c.PartitionedTableName = name
	return c
}

// SetPartitionColumns sets the PartitionColumns value of CreatePartitionedTableRequest.
func (c *CreatePartitionedTableRequest) SetPartitionColumns(partitionColumns []string) *CreatePartitionedTableRequest {
	c.PartitionColumns = partitionColumns
	return c
}

// CreateTableRequest is the request struct of CreateTable api.
type CreateTableRequest struct {
	SrcTable           string
	DimensionTableName string
	SortColumns        []string
}

// SetSortColumns sets the SortColumns value of CreateTableRequest.
func (c *CreateTableRequest) SetSortColumns(cols []string) *CreateTableRequest {
	c.SortColumns = cols
	return c
}

// SetSrcTable sets the SrcTable value of ClearAllCacheRequest.
func (c *CreateTableRequest) SetSrcTable(name string) *CreateTableRequest {
	c.SrcTable = name
	return c
}

// SetDimensionTableName sets the DimensionTableName value of ClearAllCacheRequest.
func (c *CreateTableRequest) SetDimensionTableName(name string) *CreateTableRequest {
	c.DimensionTableName = name
	return c
}

// TableRequest is the request struct of Table api.
type TableRequest struct {
	TableName string

	TableParams []TableParam
}

// TableParam stores the params for Table api.
type TableParam struct {
	Key   string
	Value string
}

// SetTableName sets the TableName value of TableRequest.
func (t *TableRequest) SetTableName(name string) *TableRequest {
	t.TableName = name
	return t
}

// SetTableParams sets the TableParams value of TableRequest.
func (t *TableRequest) SetTableParams(params []TableParam) *TableRequest {
	t.TableParams = params
	return t
}

// AddTableParam adds an element to the TableParams value of TableRequest.
func (t *TableRequest) AddTableParam(key, value string) *TableRequest {
	if t.TableParams == nil {
		t.TableParams = make([]TableParam, 0)
	}

	t.TableParams = append(t.TableParams, TableParam{key, value})
	return t
}

// TableWithCapacityRequest is the request struct of TableWithCapacity api.
type TableWithCapacityRequest struct {
	TableName string
	Capacity  int32
	Size      int32
	ColNames  []string
	ColTypes  []string
}

// SetTableName sets the TableName value of TableWithCapacityRequest.
func (t *TableWithCapacityRequest) SetTableName(name string) *TableWithCapacityRequest {
	t.TableName = name
	return t
}

// SetSize sets the Size value of TableWithCapacityRequest.
func (t *TableWithCapacityRequest) SetSize(size int32) *TableWithCapacityRequest {
	t.Size = size
	return t
}

// SetColNames sets the ColNames value of TableWithCapacityRequest.
func (t *TableWithCapacityRequest) SetColNames(colNames []string) *TableWithCapacityRequest {
	t.ColNames = colNames
	return t
}

// SetColTypes sets the ColTypes value of TableWithCapacityRequest.
func (t *TableWithCapacityRequest) SetColTypes(colTypes []string) *TableWithCapacityRequest {
	t.ColTypes = colTypes
	return t
}

// SetCapacity sets the Capacity value of TableWithCapacityRequest.
func (t *TableWithCapacityRequest) SetCapacity(capacity int32) *TableWithCapacityRequest {
	t.Capacity = capacity
	return t
}

// DropTableRequest is the request struct of DropTable api.
// If you have declared a DBHandle before, you can set it,
// or you should set the DBPath.
type DropTableRequest struct {
	TableName string
	DBHandle  string
	DBPath    string
}

// SetTableName sets the TableName value of DropTableRequest.
func (d *DropTableRequest) SetTableName(name string) *DropTableRequest {
	d.TableName = name
	return d
}

// SetDBPath sets the DBPath value of DropTableRequest.
func (d *DropTableRequest) SetDBPath(path string) *DropTableRequest {
	d.DBPath = path
	return d
}

// SetDBHandle sets the DBHandle value of DropTableRequest.
func (d *DropTableRequest) SetDBHandle(dbHandle string) *DropTableRequest {
	d.DBHandle = dbHandle
	return d
}

// DropPartitionRequest is the request struct of DropPartition api.
// If you have declared a DBHandle before, you can set it,
// or you should set the DBPath.
type DropPartitionRequest struct {
	DBHandle       string
	DBPath         string
	TableName      string
	PartitionPaths string
}

// SetTableName sets the TableName value of DropPartitionRequest.
func (d *DropPartitionRequest) SetTableName(name string) *DropPartitionRequest {
	d.TableName = name
	return d
}

// SetDBPath sets the DBPath value of DropPartitionRequest.
func (d *DropPartitionRequest) SetDBPath(path string) *DropPartitionRequest {
	d.DBPath = path
	return d
}

// SetDBHandle sets the DBHandle value of DropPartitionRequest.
func (d *DropPartitionRequest) SetDBHandle(dbHandle string) *DropPartitionRequest {
	d.DBHandle = dbHandle
	return d
}

// SetPartitionPaths sets the PartitionPaths value of DropPartitionRequest.
func (d *DropPartitionRequest) SetPartitionPaths(partitionPaths string) *DropPartitionRequest {
	d.PartitionPaths = partitionPaths
	return d
}

// LoginRequest is the request struct of Login api.
type LoginRequest struct {
	UserID   string
	Password string
}

// SetUserID sets the UserID value of LoginRequest.
func (l *LoginRequest) SetUserID(name string) *LoginRequest {
	l.UserID = name
	return l
}

// SetPassword sets the Password value of LoginRequest.
func (l *LoginRequest) SetPassword(password string) *LoginRequest {
	l.Password = password
	return l
}

// UndefRequest is the request struct of Undef api.
type UndefRequest struct {
	Obj     string
	ObjType string
}

// SetObj sets the Obj value of UndefRequest.
func (l *UndefRequest) SetObj(obj string) *UndefRequest {
	l.Obj = obj
	return l
}

// SetObjType sets the ObjType value of UndefRequest.
func (l *UndefRequest) SetObjType(objType string) *UndefRequest {
	l.ObjType = objType
	return l
}

// ClearAllCacheRequest is the request struct of ClearAllCache api.
type ClearAllCacheRequest struct {
	IsDFS bool
}

// SetIsDFS sets the IsDFS value of ClearAllCacheRequest.
func (l *ClearAllCacheRequest) SetIsDFS(isDFS bool) *ClearAllCacheRequest {
	l.IsDFS = isDFS
	return l
}
