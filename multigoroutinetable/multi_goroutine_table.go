package multigoroutinetable

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
		"time"

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
	partitionTypeList                                       chan model.DataTypeList

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

	err = mtt.generateMultiGoroutineTable(opt)
	if err != nil {
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

func (mtt *MultiGoroutineTable) generateMultiGoroutineTable(opt *Option) error {
	schema, err := mtt.getSchema(opt)
	if err != nil {
		fmt.Printf("Failed to get schema: %s\n", err.Error())
		return err
	}

	err = mtt.assignWithColDefs(schema)
	if err != nil {
		fmt.Printf("Failed to handle columns of the table returned by function schema: %s\n", err.Error())
		return err
	}

	dt, err := schema.Get("partitionColumnName")
	if err != nil {
		if !strings.Contains(err.Error(), "invalid key") {
			fmt.Printf("Failed to get partitionColumnName: %s\n", err.Error())
			return err
		}

		err = mtt.assignForNonPartitionTable(opt)
	} else {
		err = mtt.assignForPartitionTable(dt, schema, opt)
	}

	return err
}

// Insert inserts data into the table.
// The length of args must be equal with the number of columns of the table.
func (mtt *MultiGoroutineTable) Insert(args ...interface{}) error {
	if mtt.isExit() {
		return errors.New("goroutine already exits")
	}

	if len(args) != len(mtt.colTypes) {
		return errors.New("column counts don't match")
	}

	goroutineInd, err := mtt.getGoroutineInd(args)
	if err != nil {
		fmt.Printf("Failed to get goroutine index: %s\n", err.Error())
		return err
	}

	err = mtt.insertInterfaceToGoroutine(goroutineInd, args)
	if err != nil {
		fmt.Printf("Failed to insert interface: %s\n", err.Error())
		return err
	}

	return nil
}

func (mtt *MultiGoroutineTable) getDataTypes(args ...interface{}) ([]model.DataType, error) {
	prow := make([]model.DataType, len(args))
	for k, v := range args {
		d, err := getDataType(model.DataTypeByte(mtt.colTypes[k]), v)
		if err != nil {
			fmt.Printf("Failed to instantiate DataType with arg: %s\n", err.Error())
			return prow, err
		}

		prow[k] = d
	}

	return prow, nil
}

func getDataType(dt model.DataTypeByte, v interface{}) (model.DataType, error) {
	if d, ok := v.(model.DataType); ok {
		return d, nil
	}

	if dt > 64 {
		if v == nil {
			dtl := model.NewEmptyDataTypeList(dt, 1)
			vct := model.NewVector(dtl)
			return model.NewDataType(model.DtAny, vct)
		}

		dtl, err := model.NewDataTypeListFromRawData(dt, v)
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
		IsExit:              mtt.isExit(),
		GoroutineStatusList: make([]*GoroutineStatus, len(mtt.goroutines)),
		GoroutineStatus:     make([]*GoroutineStatus, len(mtt.goroutines)),
	}

	for k, v := range mtt.goroutines {
		ts := new(GoroutineStatus)
		v.getStatus(ts)
		s.SentRows += ts.SentRows
		s.UnSentRows += ts.UnSentRows
		s.UnsentRows += ts.UnsentRows
		s.FailedRows += ts.FailedRows
		s.GoroutineStatusList[k] = ts
		s.GoroutineStatus[k] = ts
	}

	return s
}

// GetUnwrittenData returns the total of unsent data and failed data.
func (mtt *MultiGoroutineTable) GetUnwrittenData() [][]interface{} {
	data := make([][]interface{}, 0)
	for _, v := range mtt.goroutines {
		if val := v.failedQueue.popAll(); val != nil {
			data = append(data, val...)
		}

		if val := v.writeQueue.popAll(); val != nil {
			data = append(data, val...)
		}
	}

	return data
}

func (mtt *MultiGoroutineTable) mockInterface(v []interface{}) ([]interface{}, int, error) {
	ret := make([]interface{}, 0)
	count := -1
	for ind, dt := range mtt.colTypes {
		switch model.DataTypeByte(dt) {
		case model.DtBool:
			_, ok := v[ind].([]byte)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect byte slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]byte))
			ret = append(ret, v[ind].([]byte)[0])
		case model.DtBlob:
			_, ok := v[ind].([][]byte)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect []byte slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([][]byte))
			ret = append(ret, v[ind].([][]byte)[0])
		case model.DtChar, model.DtCompress:
			_, ok := v[ind].([]byte)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect []byte slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]byte))
			ret = append(ret, v[ind].([]byte)[0])
		case model.DtComplex, model.DtPoint:
			_, ok := v[ind].([][2]float64)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect [2]float64 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]float64))
			ret = append(ret, v[ind].([][2]float64)[0])
		case model.DtShort:
			_, ok := v[ind].([]int16)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect int16 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]int16))
			ret = append(ret, v[ind].([]int16)[0])
		case model.DtInt:
			_, ok := v[ind].([]int32)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect int32 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]int32))
			ret = append(ret, v[ind].([]int32)[0])
		case model.DtLong:
			_, ok := v[ind].([]int64)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect int64 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]int64))
			ret = append(ret, v[ind].([]int64)[0])
		case model.DtFloat:
			_, ok := v[ind].([]float32)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect float32 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]float32))
			ret = append(ret, v[ind].([]float32)[0])
		case model.DtDouble:
			_, ok := v[ind].([]float64)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect float64 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]float64))
			ret = append(ret, v[ind].([]float64)[0])
		case model.DtDecimal32:
			_, ok := v[ind].([]*model.Decimal32)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect Decimal32 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]*model.Decimal32))
			ret = append(ret, v[ind].([]*model.Decimal32)[0])
		case model.DtDecimal64:
			_, ok := v[ind].([]*model.Decimal64)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect Decimal64 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]*model.Decimal64))
			ret = append(ret, v[ind].([]*model.Decimal64)[0])
		case model.DtDecimal128:
			_, ok := v[ind].([]*model.Decimal128)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect Decimal128 slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]*model.Decimal128))
			ret = append(ret, v[ind].([]*model.Decimal128)[0])
		case model.DtDate, model.DtDateHour, model.DtDateMinute, model.DtDatetime, model.DtMinute, model.DtMonth, model.DtNanoTime, model.DtSecond, model.DtTime, model.DtTimestamp, model.DtNanoTimestamp:
			_, ok := v[ind].([]time.Time)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect time.Time slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]time.Time))
			ret = append(ret, v[ind].([]time.Time)[0])
		case model.DtUUID, model.DtSymbol, model.DtString, model.DtDuration, model.DtInt128, model.DtIP:
			_, ok := v[ind].([]string)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect string slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]string))
			ret = append(ret, v[ind].([]string)[0])
		case model.DtAny:
			_, ok := v[ind].([]model.DataForm)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect DataForm slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]model.DataForm))
			ret = append(ret, v[ind].([]model.DataForm)[0])
		default:
			_, ok := v[ind].([]model.DataType)
			if !ok {
				return nil, 0, fmt.Errorf("col %d of type %s expect DataType slice", ind, model.GetDataTypeString(model.DataTypeByte(dt)))
			}
			count = len(v[ind].([]model.DataType))
			ret = append(ret, v[ind].([]model.DataType)[0])
		}
	}
	return ret, count, nil
}

// InsertUnwrittenData inserts data into the table.
// You can insert data obtained from GetUnwrittenData with this function.
func (mtt *MultiGoroutineTable) InsertUnwrittenData(records [][]interface{}) error {
	if mtt.isExit() {
		return errors.New("goroutine already exits")
	}
	for _, v := range records {
		mock, count, err := mtt.mockInterface(v)
		if err != nil {
			return err
		}
		goroutineInd, err := mtt.getGoroutineInd(mock)
		wt := mtt.goroutines[goroutineInd]
		if err != nil {
			fmt.Printf("Failed to get goroutine index: %s\n", err.Error())
			return err
		}
		mtt.goroutines[goroutineInd].writeQueue.addBatch(v, count)
		wt.signal.Signal()
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

func (mtt *MultiGoroutineTable) assignWithColDefs(schema *model.Dictionary) error {
	dt, err := schema.Get(colDefs)
	if err != nil {
		fmt.Printf("Failed to get cofDefs: %s\n", err.Error())
		return err
	}

	colDefs := dt.Value().(*model.Table)
	colDefsName := colDefs.GetColumnByName(colDefsName)
	mtt.colNames = colDefsName.Data.StringList()

	colDefsTypeInt := colDefs.GetColumnByName(typeInt)
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

func (mtt *MultiGoroutineTable) parseSchemaWithScalarValue(partColNames model.DataForm, schema *model.Dictionary, partitionCol string) (model.DataForm, int32, error) {
	s := partColNames.(*model.Scalar)
	if realStr := s.DataType.String(); realStr != partitionCol {
		return nil, 0, fmt.Errorf("the parameter PartitionCol must be the partitioning column %s in the table", realStr)
	}

	dt, err := schema.Get(partitionColumnIndex)
	if err != nil {
		fmt.Printf("Failed to get partitionColumnIndex: %s\n", err.Error())
		return nil, 0, err
	}

	mtt.partitionColumnIdx = dt.Value().(*model.Scalar).DataType.Value().(int32)

	dt, err = schema.Get(partitionSchema)
	if err != nil {
		fmt.Printf("Failed to get partitionSchema: %s\n", err.Error())
		return nil, 0, err
	}

	partitionSchema := dt.Value().(model.DataForm)

	dt, err = schema.Get(partitionType)
	if err != nil {
		fmt.Printf("Failed to get partitionType: %s\n", err.Error())
		return nil, 0, err
	}

	return partitionSchema, dt.Value().(*model.Scalar).DataType.Value().(int32), nil
}

func (mtt *MultiGoroutineTable) getPartitionColumnIndex(partColNames model.DataForm, partitionCol string) (int, error) {
	vct := partColNames.(*model.Vector)
	names := vct.Data.StringList()
	ind := -1
	for k, v := range names {
		if v == partitionCol {
			ind = k
			break
		}
	}

	if ind == -1 {
		return 0, errors.New("the parameter partitionCol must be the partitioning columns in the partitioned table")
	}

	return ind, nil
}

func (mtt *MultiGoroutineTable) parseSchemaWithVectorValue(partColNames model.DataForm, schema *model.Dictionary, partitionCol string) (model.DataForm, int32, error) {
	dims := partColNames.Rows()
	if dims > 1 && partitionCol == "" {
		return nil, 0, errors.New("the parameter partitionCol must be specified for a partitioned table")
	}

	ind, err := mtt.getPartitionColumnIndex(partColNames, partitionCol)
	if err != nil {
		return nil, 0, err
	}

	dt, err := schema.Get(partitionColumnIndex)
	if err != nil {
		fmt.Printf("Failed to get partitionColumnIndex: %s\n", err.Error())
		return nil, 0, err
	}

	mtt.partitionColumnIdx = dt.Value().(*model.Vector).Data.ElementValue(ind).(int32)

	dt, err = schema.Get(partitionSchema)
	if err != nil {
		fmt.Printf("Failed to get partitionSchema: %s\n", err.Error())
		return nil, 0, err
	}

	partitionSchema := dt.Value().(*model.Vector).Data.ElementValue(ind).(model.DataForm)

	dt, err = schema.Get(partitionType)
	if err != nil {
		fmt.Printf("Failed to get partitionType: %s\n", err.Error())
		return nil, 0, err
	}

	return partitionSchema, dt.Value().(*model.Vector).Data.ElementValue(ind).(int32), nil
}

func (mtt *MultiGoroutineTable) parseSchema(partColNames model.DataForm, schema *model.Dictionary, partitionCol string) (model.DataForm, int32, error) {
	if partColNames.GetDataForm() == model.DfScalar {
		return mtt.parseSchemaWithScalarValue(partColNames, schema, partitionCol)
	}

	return mtt.parseSchemaWithVectorValue(partColNames, schema, partitionCol)
}

func (mtt *MultiGoroutineTable) getSchema(opt *Option) (*model.Dictionary, error) {
	conn, err := dialer.NewSimpleConn(context.TODO(), opt.Address, opt.UserID, opt.Password)
	if err != nil {
		fmt.Printf("Failed to instantiate a simple connection: %s\n", err.Error())
		return nil, err
	}

	defer conn.Close()

	df, err := conn.RunScript(mtt.getSchemaScript(opt))
	if err != nil {
		fmt.Printf("Failed to call function schema with the specified table %s: %s\n", opt.TableName, err.Error())
		return nil, err
	}

	return df.(*model.Dictionary), nil
}

func (mtt *MultiGoroutineTable) getSchemaScript(opt *Option) string {
	if opt.Database == "" {
		return fmt.Sprintf("schema(%s)", opt.TableName)
	}

	return fmt.Sprintf("schema(loadTable(\"%s\",\"%s\"))", opt.Database, opt.TableName)
}

func (mtt *MultiGoroutineTable) assignForPartitionTable(dt model.DataType, schema *model.Dictionary, opt *Option) error {
	mtt.isPartition = true
	partColNames := dt.Value().(model.DataForm)

	partitionSchema, partitionType, err := mtt.parseSchema(partColNames, schema, opt.PartitionCol)
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
	chanNum := 16
	if len(mtt.goroutines) > chanNum {
		chanNum = len(mtt.goroutines)
	}
	mtt.partitionTypeList = make(chan model.DataTypeList, chanNum)
	for i := 0; i < chanNum; i++ {
		mtt.partitionTypeList <- model.NewEmptyDataTypeList(model.DataTypeByte(mtt.colTypes[mtt.partitionColumnIdx]), 1)
	}

	return nil
}

func (mtt *MultiGoroutineTable) assignForNonPartitionTable(opt *Option) error {
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
	if err := validateOption(opt); err != nil {
		return nil, err
	}

	mtt := &MultiGoroutineTable{
		database:   opt.Database,
		tableName:  opt.TableName,
		batchSize:  opt.BatchSize,
		throttle:   opt.Throttle,
		hasError:   false,
		goroutines: make([]*writerGoroutine, opt.GoroutineCount),
	}

	return mtt, nil
}

func validateOption(opt *Option) error {
	if opt.GoroutineCount < 1 {
		return errors.New("the parameter GoroutineCount must be greater than or equal to 1")
	}

	if opt.BatchSize < 1 {
		return errors.New("the parameter BatchSize must be greater than or equal to 1")
	}

	if opt.Throttle < 1 {
		return errors.New("the parameter Throttle must be greater than or equal to 0")
	}

	if opt.GoroutineCount > 1 && len(opt.PartitionCol) < 1 {
		return errors.New("the parameter PartitionCol must be specified when GoroutineCount is greater than 1")
	}

	return nil
}

func (mtt *MultiGoroutineTable) getGoroutineIndForPartitionTable(prow []interface{}) (int, error) {
	var goroutineInd int
	s := prow[mtt.partitionColumnIdx]
	if s != nil {
		select {
		case list := <-mtt.partitionTypeList:
			defer func() {
				mtt.partitionTypeList <- list
			}()
			err := list.SetWithRawData(0, s)
			if err != nil {
				return 0, err
			}
			pvc := model.NewVector(list)
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
		default:
			partitionValue, err := getDataType(model.DataTypeByte(mtt.colTypes[mtt.partitionColumnIdx]), s)
			if err != nil {
				return -1, err
			}
			dtl := model.NewDataTypeList(partitionValue.DataType(), []model.DataType{partitionValue})
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
		}
	} else {
		goroutineInd = 0
	}

	return goroutineInd, nil
}

func (mtt *MultiGoroutineTable) getGoroutineIndForNonPartitionTable(prow []interface{}) (int, error) {
	var goroutineInd int
	if prow[mtt.goroutineByColIndexForNonPartition] != nil {
		s := prow[mtt.goroutineByColIndexForNonPartition]
		partitionValue, err := getDataType(model.DataTypeByte(mtt.colTypes[mtt.goroutineByColIndexForNonPartition]), s)
		if err != nil {
			return -1, err
		}
		dtl := model.NewDataTypeList(partitionValue.DataType(), []model.DataType{partitionValue})
		pvc := model.NewVector(dtl)
		goroutineInd = pvc.HashBucket(0, len(mtt.goroutines))
	} else {
		goroutineInd = 0
	}

	return goroutineInd, nil
}

func (mtt *MultiGoroutineTable) getGoroutineInd(prow []interface{}) (int, error) {
	var goroutineInd int
	var err error
	if len(mtt.goroutines) > 1 {
		if mtt.isPartition {
			goroutineInd, err = mtt.getGoroutineIndForPartitionTable(prow)
		} else {
			goroutineInd, err = mtt.getGoroutineIndForNonPartitionTable(prow)
		}
		goroutineInd = goroutineInd % len(mtt.goroutines)
	} else {
		goroutineInd = 0
	}

	return goroutineInd, err
}

func (mtt *MultiGoroutineTable) insertInterfaceToGoroutine(hashKey int, prow []interface{}) error {
	if hashKey < 0 {
		hashKey = 0
	}

	ind := hashKey % len(mtt.goroutines)
	wt := mtt.goroutines[ind]
	err := wt.writeQueue.add(prow)
	if err != nil {
		return err
	}

	wt.signal.Signal()
	return nil
}

func (mtt *MultiGoroutineTable) isExit() bool {
	return mtt.hasError
}
