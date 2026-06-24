package api

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dolphindb/api-go/v3/domain"
	"github.com/dolphindb/api-go/v3/model"
)

// PartitionedTableAppender is used to append tables into a partitioned table.
type PartitionedTableAppender struct {
	partitionColumnIdx int32
	cols               int
	goroutineCount     int
	partitionType      int32
	chunkIndices       [][]int
	appendScript       string

	columnCategories []model.CategoryString
	columnTypes      []model.DataTypeByte

	pool             *DBConnectionPool
	partitionSchema  model.DataForm
	tableInfo        *model.Dictionary
	domain           domain.Domain
	partitionColType model.DataTypeByte
}

// PartitionedTableAppenderOption is the options of PartitionedTableAppender.
type PartitionedTableAppenderOption struct {
	// DBPath of partitioned table
	DBPath string
	// Name of partitioned table
	TableName string
	// the partitioning column name
	PartitionCol string
	// the method used to append the table
	AppendFunction string

	// object of DBConnectionPool
	Pool *DBConnectionPool
}

// NewPartitionedTableAppender instantiates a new PartitionedTableAppender according to the option.
func NewPartitionedTableAppender(opt *PartitionedTableAppenderOption) (*PartitionedTableAppender, error) {
	if err := validatePartitionedTableAppenderOption(opt); err != nil {
		return nil, err
	}

	res, task := initPartitionedTableAppender(opt)
	err := res.pool.ExecuteTask(task)
	if err != nil {
		apiLogErrorf("failed to execute task: %v", err)
		return nil, err
	}

	if !task.IsSuccess() {
		apiLogErrorf("task is not success: %v", task.err)
		return nil, task.err
	}

	err = res.packAppenderWithPartitionColumnName(task, opt)
	if err != nil {
		apiLogErrorf("failed to handle PartitionColumnName: %v", err)
		return nil, err
	}

	err = packAppenderWithColDefs(res)
	if err != nil {
		return nil, err
	}

	res.domain, err = domain.CreateDomain(domain.GetPartitionType(int(res.partitionType)), res.partitionColType, res.partitionSchema)
	return res, err
}

func validatePartitionedTableAppenderOption(opt *PartitionedTableAppenderOption) error {
	if opt == nil {
		return fmt.Errorf("partitioned table appender option must not be nil")
	}
	if opt.Pool == nil {
		return fmt.Errorf("partitioned table appender connection pool must not be nil")
	}
	if strings.TrimSpace(opt.TableName) == "" {
		return fmt.Errorf("partitioned table appender table name must not be empty")
	}
	if strings.TrimSpace(opt.PartitionCol) == "" {
		return fmt.Errorf("partitioned table appender partition column must not be empty")
	}

	return nil
}

func packAppenderWithColDefs(pta *PartitionedTableAppender) error {
	dt, err := pta.tableInfo.Get("colDefs")
	if err != nil {
		apiLogErrorf("failed to get colDefs from table: %v", err)
		return err
	}

	tb := dt.Value().(*model.Table)
	pta.cols = tb.Rows()
	pta.columnCategories = make([]model.CategoryString, pta.cols)
	pta.columnTypes = make([]model.DataTypeByte, pta.cols)

	vct := tb.GetColumnByName("typeInt")
	for i := 0; i < pta.cols; i++ {
		raw := vct.Data.ElementValue(i)
		pta.columnTypes[i] = model.DataTypeByte(raw.(int32))
		pta.columnCategories[i] = model.GetCategory(pta.columnTypes[i])
	}

	return nil
}

// Close closes the connection pool.
func (p *PartitionedTableAppender) Close() error {
	if p.pool.isClosed {
		return nil
	}

	return p.pool.Close()
}

// Append appends the table to the partitioned table which has been set when calling NewPartitionedTableAppender.
func (p *PartitionedTableAppender) Append(tb *model.Table) (int, error) {
	if err := p.checkTable(tb); err != nil {
		return 0, err
	}

	for i := 0; i < p.goroutineCount; i++ {
		p.chunkIndices[i] = make([]int, 0)
	}

	keys, err := p.domain.GetPartitionKeys(tb.GetColumnByIndex(int(p.partitionColumnIdx)))
	if err != nil {
		apiLogErrorf("failed to call GetPartitionKeys: %v", err)
		return 0, err
	}

	for k, v := range keys {
		if v >= 0 {
			p.chunkIndices[v%p.goroutineCount] = append(p.chunkIndices[v%p.goroutineCount], k)
		}
	}

	tasks := p.packTasks(tb)
	err = p.pool.Execute(tasks)
	if err != nil {
		apiLogErrorf("failed to execute tasks: %v", err)
		return 0, err
	}

	return p.calAffected(tasks)
}

func (p *PartitionedTableAppender) checkTable(tb *model.Table) error {
	if p.cols != tb.Columns() {
		return errors.New("the input table doesn't match the schema of the target table")
	}

	for i := 0; i < p.cols; i++ {
		curCol := tb.GetColumnByIndex(i)
		colDateType := curCol.GetDataType()
		err := p.checkColumnType(i, model.GetCategory(colDateType), colDateType)
		if err != nil {
			apiLogErrorf("failed to check column type: %v", err)
			return err
		}
	}

	return nil
}

func (p *PartitionedTableAppender) calAffected(tasks []*Task) (int, error) {
	affected := 0
	for _, v := range tasks {
		if v == nil {
			continue
		}

		if !v.IsSuccess() {
			return 0, v.err
		}

		re := v.GetResult()
		if re.GetDataType() == model.DtVoid {
			affected += 0
		} else {
			sca := re.(*model.Scalar)
			val := sca.Value()
			affected += int(val.(int32))
		}
	}

	return affected, nil
}

func (p *PartitionedTableAppender) packTasks(tb *model.Table) []*Task {
	tasks := make([]*Task, p.goroutineCount)
	for i := 0; i < p.goroutineCount; i++ {
		chunk := p.chunkIndices[i]
		if len(chunk) == 0 {
			continue
		}

		array := make([]int, len(chunk))
		copy(array, chunk)
		tasks[i] = &Task{
			Script: p.appendScript,
			Args:   []model.DataForm{tb.GetSubtable(array)},
		}
	}

	return tasks
}

func initPartitionedTableAppender(opt *PartitionedTableAppenderOption) (*PartitionedTableAppender, *Task) {
	res := &PartitionedTableAppender{
		pool:           opt.Pool,
		goroutineCount: opt.Pool.GetPoolSize(),
	}

	res.chunkIndices = make([][]int, res.goroutineCount)
	for k := range res.chunkIndices {
		res.chunkIndices[k] = make([]int, 0)
	}

	task := &Task{}
	if opt.DBPath == "" {
		task.Script = fmt.Sprintf("schema(%s)", opt.TableName)
		res.appendScript = fmt.Sprintf("tableInsert{%s}", opt.TableName)
	} else {
		task.Script = fmt.Sprintf("schema(loadTable(\"%s\", \"%s\"))", opt.DBPath, opt.TableName)
		res.appendScript = fmt.Sprintf("tableInsert{loadTable(\"%s\", \"%s\")}", opt.DBPath, opt.TableName)
	}

	if opt.AppendFunction != "" {
		res.appendScript = opt.AppendFunction
	}

	return res, task
}

func (p *PartitionedTableAppender) packAppenderWithPartitionColumnName(task *Task, opt *PartitionedTableAppenderOption) error {
	p.tableInfo = task.GetResult().(*model.Dictionary)
	dt, err := p.tableInfo.Get("partitionColumnName")
	if err != nil {
		apiLogErrorf("failed to get partitionColumnName: %v", err)
		return err
	}

	partColNames := dt.Value().(model.DataForm)
	if partColNames == nil {
		return errors.New("can't find specified partition column name")
	}

	if partColNames.GetDataForm() == model.DfScalar {
		err = p.packAppenderWithScalarPartitionColumnName(partColNames, opt)
	} else {
		err = p.packAppenderWithVectorPartitionColumnName(partColNames, opt)
	}

	return err
}

func (p *PartitionedTableAppender) packAppenderWithScalarPartitionColumnName(partColNames model.DataForm, opt *PartitionedTableAppenderOption) error {
	var err error

	sca := partColNames.(*model.Scalar)
	if name := sca.DataType.String(); name != opt.PartitionCol {
		return errors.New("can't find specified partition column name")
	}

	p.partitionColumnIdx, err = getInt32ValueFromDictionary(p.tableInfo, "partitionColumnIndex")
	if err != nil {
		apiLogErrorf("failed to get partitionColumnIndex from dictionary: %v", err)
		return err
	}

	dt, err := p.tableInfo.Get("partitionSchema")
	if err != nil {
		return err
	}

	p.partitionSchema = dt.Value().(model.DataForm)

	p.partitionType, err = getInt32ValueFromDictionary(p.tableInfo, "partitionType")
	if err != nil {
		apiLogErrorf("failed to get partitionType from dictionary: %v", err)
		return err
	}

	val, err := getInt32ValueFromDictionary(p.tableInfo, "partitionColumnType")
	if err != nil {
		apiLogErrorf("failed to get partitionColumnType from dictionary: %v", err)
		return err
	}

	p.partitionColType = model.DataTypeByte(val)
	return nil
}

func (p *PartitionedTableAppender) packAppenderWithVectorPartitionColumnName(partColNames model.DataForm, opt *PartitionedTableAppenderOption) error {
	var err error

	vct := partColNames.(*model.Vector)
	names := vct.Data.StringList()
	ind := -1
	for k, v := range names {
		if strings.EqualFold(v, opt.PartitionCol) {
			ind = k
			break
		}
	}

	if ind < 0 {
		return errors.New("can't find specified partition column name")
	}

	p.partitionColumnIdx, err = getInt32ValueFromTableWithInd(p.tableInfo, "partitionColumnIndex", ind)
	if err != nil {
		apiLogErrorf("failed to get partitionColumnIndex from dictionary with ind %d: %v", ind, err)
		return err
	}

	dt, err := p.tableInfo.Get("partitionSchema")
	if err != nil {
		apiLogErrorf("failed to get partitionSchema: %v", err)
		return err
	}

	vct = dt.Value().(*model.Vector)
	p.partitionSchema = vct.Data.ElementValue(ind).(model.DataForm)

	p.partitionType, err = getInt32ValueFromTableWithInd(p.tableInfo, "partitionType", ind)
	if err != nil {
		apiLogErrorf("failed to get partitionType from dictionary with ind %d: %v", ind, err)
		return err
	}

	val, err := getInt32ValueFromTableWithInd(p.tableInfo, "partitionColumnType", ind)
	if err != nil {
		apiLogErrorf("failed to get partitionColumnType from dictionary with ind %d: %v", ind, err)
		return err
	}

	p.partitionColType = model.DataTypeByte(val)
	return nil
}

func (p *PartitionedTableAppender) checkColumnType(col int, cat model.CategoryString, dt model.DataTypeByte) error {
	expectCategory := p.columnCategories[col]
	expectType := p.columnTypes[col]
	if cat != expectCategory {
		return fmt.Errorf("column %d, expect category %s, got category %s", col, expectCategory, cat)
	} else if cat == model.TEMPORAL && dt != expectType {
		return fmt.Errorf("column %d, temporal column must have exactly the same type, expect %s, got %s",
			col, model.GetDataTypeString(expectType), model.GetDataTypeString(dt))
	}

	return nil
}

func getInt32ValueFromTableWithInd(dict *model.Dictionary, colName string, ind int) (int32, error) {
	dt, err := dict.Get(colName)
	if err != nil {
		apiLogErrorf("failed to get %s from dictionary: %v", colName, err)
		return 0, err
	}

	vct := dt.Value().(*model.Vector)
	val := vct.Data.ElementValue(ind)
	return val.(int32), nil
}

func getInt32ValueFromDictionary(dict *model.Dictionary, colName string) (int32, error) {
	dt, err := dict.Get(colName)
	if err != nil {
		apiLogErrorf("failed to get %s from dictionary: %v", colName, err)
		return 0, err
	}

	s := dt.Value().(*model.Scalar)
	val := s.DataType.Value()
	return val.(int32), nil
}
