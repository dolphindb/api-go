package multigoroutinetable

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/domain"
	"github.com/dolphindb/api-go/model"
)

// MultiGoroutineTable is used to insert data into a table with multiple goroutines.
type MultiGoroutineTable struct {
	database, tableName, errorInfo                          string
	batchSize, throttle, goroutineByColIndexForNonPartition int
	hasError, isPartition                                   bool
	colNames                                                []string
	colTypes                                                []int
	partitionColumnIdx                                      int32

	partitionDomain domain.Domain
	goroutines      []*writerGoroutine
}

// Option is used to configure MultiGoroutineTable.
type Option struct {
	// database path or database handle
	Database string
	// name of the table
	TableName string
	// address of the dolphindb server
	Address string
	// user id
	UserID string
	// password of the user
	Password string
	// the amount of data processed at one time
	BatchSize int
	// timeout. unit: millisecond
	Throttle int
	// the number of coroutine
	GoroutineCount int
	// the partitioning column name
	PartitionCol string
}

// NewMultiGoroutineTable instantiates an instance of MultiGoroutineTable with MultiGoroutineTableOption.
func NewMultiGoroutineTable(opt *Option) (*MultiGoroutineTable, error) {
	mtt, err := initMultiGoroutineTable(opt)
	if err != nil {
		fmt.Printf("Failed to instantiate MultiGoroutineTable: %s\n", err.Error())
		return nil, err
	}

	schema, err := mtt.getSchema(opt)
	if err != nil {
		fmt.Printf("Failed to get schema: %s\n", err.Error())
		return nil, err
	}

	err = mtt.handleSchemaColumn(schema)
	if err != nil {
		fmt.Printf("Failed to handle columns of the table returned by function schema: %s\n", err.Error())
		return nil, err
	}

	err = mtt.handlePartitionColumnName(schema, opt)
	if err != nil {
		fmt.Printf("Failed to handle PartitionColumnName: %s\n", err.Error())
		return nil, err
	}

	for i := 0; i < opt.GoroutineCount; i++ {
		conn, err := dialer.NewSimpleConn(context.TODO(), opt.Address, opt.UserID, opt.Password)
		if err != nil {
			fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
			return nil, err
		}

		wt := newWriterGoroutine(i, mtt, conn)
		mtt.goroutines[i] = wt
	}

	return mtt, nil
}

// Insert inserts data into the table.
// The length of args must be equal with the number of columns of the table.
func (mtt *MultiGoroutineTable) Insert(args ...interface{}) error {
	if mtt.isExist() {
		return errors.New("goroutine already exists")
	}

	if len(args) != len(mtt.colTypes) {
		return errors.New("column counts don't match")
	}

	prow := make([]model.DataType, len(args))
	for k, v := range args {
		d, err := packDataType(model.DataTypeByte(mtt.colTypes[k]), v)
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with arg: %s\n", err.Error())
			return err
		}

		prow[k] = d
	}

	goroutineInd, err := mtt.getGoroutineInd(prow)
	if err != nil {
		fmt.Printf("Failed to get goroutine index: %s\n", err.Error())
		return err
	}

	mtt.insertGoroutineWrite(goroutineInd, prow)

	return nil
}

func packDataType(dt model.DataTypeByte, v interface{}) (model.DataType, error) {
	if d, ok := v.(model.DataType); ok {
		return d, nil
	}

	if dt > 64 {
		if v == nil {
			dtl := model.NewEmptyDataTypeList(dt, 1)
			vct := model.NewVector(dtl)
			return model.NewDataType(model.DtAny, vct)
		}

		dtl, err := model.NewDataTypeListWithRaw(dt, v)
		if err != nil {
			return nil, err
		}

		if dtl.Len() == 0 {
			dtl = model.NewEmptyDataTypeList(dt, 1)
		}

		vct := model.NewVector(dtl)
		return model.NewDataType(model.DtAny, vct)
	}

	return model.NewDataType(dt, v)
}

// GetStatus returns the status for the instance of MultiGoroutineTable.
func (mtt *MultiGoroutineTable) GetStatus() *Status {
	s := &Status{
		ErrMsg:              mtt.errorInfo,
		IsExit:              mtt.isExist(),
		GoroutineStatusList: make([]*GoroutineStatus, len(mtt.goroutines)),
	}

	for k, v := range mtt.goroutines {
		ts := new(GoroutineStatus)
		v.getStatus(ts)
		s.SentRows += ts.SentRows
		s.UnSentRows += ts.UnSentRows
		s.FailedRows += ts.FailedRows
		s.GoroutineStatusList[k] = ts
	}

	return s
}

// GetUnwrittenData returns the total of unsent data and failed data.
func (mtt *MultiGoroutineTable) GetUnwrittenData() [][]model.DataType {
	data := make([][]model.DataType, 0)
loop:
	for _, v := range mtt.goroutines {
	failed:
		for {
			select {
			case val := <-v.failedQueue.Out:
				data = append(data, val.([]model.DataType))
			default:
				break failed
			}
		}

		for {
			select {
			case val := <-v.writeQueue.Out:
				data = append(data, val.([]model.DataType))
			default:
				break loop
			}
		}
	}

	return data
}

// InsertUnwrittenData inserts data into the table.
// You can insert data obtained from GetUnwrittenData with this function.
func (mtt *MultiGoroutineTable) InsertUnwrittenData(records [][]model.DataType) error {
	if mtt.isExist() {
		return errors.New("goroutine already exists")
	}

	if len(mtt.goroutines) > 1 {
		if mtt.isPartition {
			pvc, err := mtt.packVector(records, int(mtt.partitionColumnIdx))
			if err != nil {
				fmt.Printf("Failed to pack vector: %s\n", err.Error())
				return err
			}

			goroutineIndexes, err := mtt.partitionDomain.GetPartitionKeys(pvc)
			if err != nil {
				fmt.Printf("Failed to call GetPartitionKeys: %s\n", err.Error())
				return err
			}

			for k, v := range goroutineIndexes {
				mtt.insertGoroutineWrite(v, records[k])
			}
		} else {
			pvc, err := mtt.packVector(records, mtt.goroutineByColIndexForNonPartition)
			if err != nil {
				fmt.Printf("Failed to package vector: %s\n", err.Error())
				return err
			}

			for k, v := range records {
				goroutineInd := pvc.HashBucket(k, len(mtt.goroutines))
				mtt.insertGoroutineWrite(goroutineInd, v)
			}
		}
	} else {
		for _, v := range records {
			mtt.insertGoroutineWrite(0, v)
		}
	}

	return nil
}

// WaitForGoroutineCompletion waits for the data to be sent completely and exits the MultiGoroutineTable.
// An error will be thrown if you call Insert or InsertUnwrittenData after the MultiGoroutineTable exits.
func (mtt *MultiGoroutineTable) WaitForGoroutineCompletion() {
	for _, v := range mtt.goroutines {
		v.stop()
		//nolint
		for !v.isFinished {
			// loop
		}

		if v.Conn != nil {
			v.Conn.Close()
		}

		v.Conn = nil
	}

	mtt.hasError = true
}

func (mtt *MultiGoroutineTable) handleSchemaColumn(schema *model.Dictionary) error {
	dt, err := schema.Get("colDefs")
	if err != nil {
		fmt.Printf("Failed to get cofDefs: %s\n", err.Error())
		return err
	}

	colDefs := dt.Value().(*model.Table)
	colDefsName := colDefs.GetColumnByName("name")
	mtt.colNames = colDefsName.Data.StringList()

	colDefsTypeInt := colDefs.GetColumnByName("typeInt")
	intStr := colDefsTypeInt.Data.StringList()

	mtt.colTypes = make([]int, len(intStr))
	for k, v := range intStr {
		mtt.colTypes[k], err = strconv.Atoi(v)
		if err != nil {
			fmt.Printf("Failed to parse colTypes: %s\n", err.Error())
			return err
		}
	}

	return nil
}

func (mtt *MultiGoroutineTable) getSchema(opt *Option) (*model.Dictionary, error) {
	conn, err := dialer.NewSimpleConn(context.TODO(), opt.Address, opt.UserID, opt.Password)
	if err != nil {
		fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
		return nil, err
	}

	defer conn.Close()

	var df model.DataForm
	if opt.Database == "" {
		df, err = conn.RunScript(fmt.Sprintf("schema(%s)", opt.TableName))
	} else {
		df, err = conn.RunScript(fmt.Sprintf("schema(loadTable(\"%s\",\"%s\"))", opt.Database, opt.TableName))
	}

	if err != nil {
		fmt.Printf("Failed to call function schema with the specified table %s: %s\n", opt.TableName, err.Error())
		return nil, err
	}

	return df.(*model.Dictionary), nil
}

func (mtt *MultiGoroutineTable) handlePartitionColumnName(schema *model.Dictionary, opt *Option) error {
	dt, err := schema.Get("partitionColumnName")
	if err != nil {
		if !strings.Contains(err.Error(), "invalid key") {
			fmt.Printf("Failed to get partitionColumnName: %s\n", err.Error())
			return err
		}

		err = mtt.handleNoPartitionColumnName(opt)
		if err != nil {
			return err
		}
	} else {
		mtt.isPartition = true
		partColNames := dt.Value().(model.DataForm)

		partitionSchema, partitionType, err := mtt.handlePartColNames(partColNames, schema, opt.PartitionCol)
		if err != nil {
			fmt.Printf("Failed to handle partColNames: %s\n", err.Error())
			return err
		}

		colType := mtt.colTypes[mtt.partitionColumnIdx]
		partitionColType := domain.GetPartitionType(int(partitionType))
		mtt.partitionDomain, err = domain.CreateDomain(partitionColType, model.DataTypeByte(colType), partitionSchema)
		if err != nil {
			fmt.Printf("Failed to create domain: %s\n", err.Error())
			return err
		}
	}

	return nil
}

func (mtt *MultiGoroutineTable) handleNoPartitionColumnName(opt *Option) error {
	if opt.Database != "" && opt.GoroutineCount > 1 {
		return errors.New("the parameter GoroutineCount must be 1 for a dimension table")
	}

	mtt.isPartition = false

	if opt.PartitionCol != "" {
		ind := -1
		for i := 0; i < len(mtt.colNames); i++ {
			if mtt.colNames[i] == opt.PartitionCol {
				ind = i
				break
			}
		}

		if ind < 0 {
			return fmt.Errorf("no match found for %s", opt.PartitionCol)
		}

		mtt.goroutineByColIndexForNonPartition = ind
	}

	return nil
}

func initMultiGoroutineTable(opt *Option) (*MultiGoroutineTable, error) {
	if opt.GoroutineCount < 1 {
		return nil, errors.New("the parameter GoroutineCount must be greater than or equal to 1")
	}

	mtt := &MultiGoroutineTable{
		database:   opt.Database,
		tableName:  opt.TableName,
		batchSize:  opt.BatchSize,
		throttle:   opt.Throttle,
		hasError:   false,
		goroutines: make([]*writerGoroutine, opt.GoroutineCount),
	}

	if opt.BatchSize < 1 {
		return nil, errors.New("the parameter BatchSize must be greater than or equal to 1")
	}

	if opt.Throttle < 1 {
		return nil, errors.New("the parameter Throttle must be greater than or equal to 0")
	}

	if opt.GoroutineCount > 1 && len(opt.PartitionCol) < 1 {
		return nil, errors.New("the parameter PartitionCol must be specified when GoroutineCount is greater than 1")
	}

	return mtt, nil
}

func (mtt *MultiGoroutineTable) getGoroutineInd(prow []model.DataType) (int, error) {
	var goroutineInd int
	if len(mtt.goroutines) > 1 {
		if mtt.isPartition {
			s := prow[mtt.partitionColumnIdx]
			if s != nil {
				dtl := model.NewDataTypeList(s.DataType(), []model.DataType{s})
				pvc := model.NewVector(dtl)

				indexes, err := mtt.partitionDomain.GetPartitionKeys(pvc)
				if err != nil {
					fmt.Printf("Failed to call GetPartitionKeys: %s\n", err.Error())
					return 0, err
				}

				if len(indexes) > 0 {
					goroutineInd = indexes[0]
				} else {
					return 0, errors.New("failed to obtain the partition scheme")
				}
			} else {
				goroutineInd = 0
			}
		} else {
			if prow[mtt.goroutineByColIndexForNonPartition] != nil {
				s := prow[mtt.goroutineByColIndexForNonPartition]
				dtl := model.NewDataTypeList(s.DataType(), []model.DataType{s})
				pvc := model.NewVector(dtl)
				goroutineInd = pvc.HashBucket(0, len(mtt.goroutines))
			} else {
				goroutineInd = 0
			}
		}
	} else {
		goroutineInd = 0
	}

	return goroutineInd, nil
}

func (mtt *MultiGoroutineTable) insertGoroutineWrite(hashKey int, prow []model.DataType) {
	if hashKey < 0 {
		hashKey = 0
	}

	ind := hashKey % len(mtt.goroutines)
	wt := mtt.goroutines[ind]
	wt.writeQueue.In <- prow

	select {
	case wt.signal <- true:
	default:
	}
}

func (mtt *MultiGoroutineTable) packVector(records [][]model.DataType, ind int) (*model.Vector, error) {
	dtArr := make([]model.DataType, len(records))
	dt := model.DataTypeByte(mtt.colTypes[ind])
	for k, row := range records {
		if len(row) != len(mtt.colTypes) {
			return nil, errors.New("column counts don't match")
		}

		if !isEqualDataTypeByte(row[ind].DataType(), dt) {
			return nil, fmt.Errorf("column doesn't match. Expect %s, but get %s",
				model.GetDataTypeString(row[ind].DataType()), model.GetDataTypeString(dt))
		}

		dtArr[k] = row[ind]
	}

	dtl := model.NewDataTypeList(dt, dtArr)
	return model.NewVector(dtl), nil
}

func (mtt *MultiGoroutineTable) isExist() bool {
	return mtt.hasError
}

func isEqualDataTypeByte(a, b model.DataTypeByte) bool {
	if a == b || (a == model.DtSymbol && b == model.DtString) ||
		(b == model.DtSymbol && a == model.DtString) {
		return true
	}

	return false
}
