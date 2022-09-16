package multigoroutinetable

import (
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"

	"github.com/smallnest/chanx"
)

type writerGoroutine struct {
	dialer.Conn

	signal      chan bool
	tableWriter *MultiGoroutineTable
	writeQueue  *chanx.UnboundedChan
	failedQueue *chanx.UnboundedChan

	insertScript   string
	saveScript     string
	sentRows       int
	isFinished     bool
	exit           chan bool
	goroutineIndex int
}

func newWriterGoroutine(goroutineIndex int, mtw *MultiGoroutineTable, conn dialer.Conn) *writerGoroutine {
	res := &writerGoroutine{
		goroutineIndex: goroutineIndex,
		Conn:           conn,
		tableWriter:    mtw,
		signal:         make(chan bool),
		exit:           make(chan bool),
		writeQueue:     chanx.NewUnboundedChan(mtw.batchSize),
		failedQueue:    chanx.NewUnboundedChan(mtw.batchSize),
	}

	go res.run()

	time.Sleep(1 * time.Millisecond)

	return res
}

func (w *writerGoroutine) run() {
	if !w.init() {
		return
	}

	w.exit = make(chan bool)

	for !w.isExit() {
		<-w.signal
		if !w.isExit() && w.tableWriter.batchSize > 1 && w.tableWriter.throttle > 0 {
			end := time.Now().Add(time.Duration(w.tableWriter.throttle) * time.Millisecond)
			if !w.isExit() && w.writeQueue.Len() < w.tableWriter.batchSize {
				for time.Now().Before(end) {
					// Nothing
				}
			}
		}

		for !w.isExit() && w.writeAllData() {
		}
	}

	for !w.tableWriter.isExist() && w.writeAllData() {
	}

	w.isFinished = true
}

func (w *writerGoroutine) getStatus(status *GoroutineStatus) {
	status.GoroutineIndex = w.goroutineIndex
	status.SentRows = w.sentRows
	status.UnSentRows = w.writeQueue.Len()
	status.FailedRows = w.failedQueue.Len()
}

func (w *writerGoroutine) init() bool {
	if w.tableWriter.database == "" {
		w.insertScript = fmt.Sprintf("tableInsert{\"%s\"}", w.tableWriter.tableName)
	} else {
		w.insertScript = fmt.Sprintf("tableInsert{loadTable(\"%s\",\"%s\")}", w.tableWriter.database, w.tableWriter.tableName)
	}

	return true
}

func (w *writerGoroutine) writeAllData() bool {
	items := make([][]model.DataType, 0)
loop:
	for {
		select {
		case val := <-w.writeQueue.Out:
			items = append(items, val.([]model.DataType))
		default:
			if w.writeQueue.Len() == 0 {
				break loop
			}
		}
	}

	if size := len(items); size < 1 {
		return false
	}

	defer func() {
		re := recover()
		if re != nil {
			for _, v := range items {
				w.failedQueue.In <- v
			}

			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			fmt.Println("Failed to insert data into the table: ", string(buf[:n]))
			w.tableWriter.errorInfo = fmt.Sprintf("%v", re)
		}
	}()

	addRowCount := len(items)
	writeTable, isWriteDone := w.packWriteTable(items)
	if isWriteDone && writeTable != nil && addRowCount > 0 {
		err := w.runScript(writeTable, addRowCount)
		if err != nil {
			isWriteDone = false
			w.tableWriter.errorInfo = err.Error()
			w.tableWriter.hasError = true
			if w.Conn != nil {
				w.Conn.Close()
			}

			w.Conn = nil
		}
	}

	if !isWriteDone {
		for _, v := range items {
			w.failedQueue.In <- v
		}
	}

	return true
}

func (w *writerGoroutine) packWriteTable(items [][]model.DataType) (*model.Table, bool) {
	isWriteDone := true
	colValues := make([]*model.Vector, len(w.tableWriter.colTypes))
	for k, v := range w.tableWriter.colTypes {
		var vct *model.Vector
		switch {
		case v >= 128:
			dtl := model.NewEmptyDataTypeList(model.DataTypeByte(v-128), len(items))
			vct = model.NewVector(dtl)
		case v >= 64:
			vl := make([]*model.Vector, 0)
			for i := 0; i < len(items); i++ {
				item := items[i][k].Value().(*model.Vector)
				vl = append(vl, item)
			}

			av := model.NewArrayVector(vl)
			vct = model.NewVectorWithArrayVector(av)
		default:
			dtl := model.NewEmptyDataTypeList(model.DataTypeByte(v), len(items))
			vct = model.NewVector(dtl)
		}

		colValues[k] = vct
	}

	for k, row := range items {
		for ind, col := range colValues {
			if col.ArrayVector == nil {
				err := col.Set(k, row[ind])
				if err != nil {
					fmt.Println("Failed to set DataType into Vector: ", err)
					isWriteDone = false
					w.tableWriter.hasError = true
					w.tableWriter.errorInfo = err.Error()
					break
				}
			}
		}
	}

	if isWriteDone {
		return model.NewTable(w.tableWriter.colNames, colValues), true
	}

	return nil, false
}

func (w *writerGoroutine) runScript(df model.DataForm, count int) (err error) {
	defer func() {
		raw := recover()
		if raw != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			fmt.Println("Failed to call function tableInsert: ", string(buf[:n]))
			err = raw.(error)
		}
	}()

	args := make([]model.DataForm, 1)
	args[0] = df
	_, err = w.RunFunc(w.insertScript, args)
	if err != nil {
		fmt.Printf("Failed to run func: %s\n", err.Error())
		return err
	}

	if w.saveScript != "" {
		_, err = w.RunScript(w.saveScript)
		if err != nil {
			fmt.Printf("Failed to run script: %s\n", err.Error())
			return err
		}
	}

	w.sentRows += count

	return nil
}

func (w *writerGoroutine) isExit() bool {
	select {
	case <-w.exit:
		return true
	default:
		return w.tableWriter.hasError
	}
}

func (w *writerGoroutine) stop() {
	select {
	case <-w.exit:
	default:
		close(w.exit)
	}

	select {
	case w.signal <- true:
	default:
	}
}

func (mtt *MultiGoroutineTable) handlePartColNamesScalar(partColNames model.DataForm, schema *model.Dictionary, partitionCol string) (model.DataForm, int32, error) {
	s := partColNames.(*model.Scalar)
	if realStr := s.DataType.String(); realStr != partitionCol {
		return nil, 0, fmt.Errorf("the parameter PartitionCol must be the partitioning column %s in the table", realStr)
	}

	dt, err := schema.Get("partitionColumnIndex")
	if err != nil {
		fmt.Printf("Failed to get partitionColumnIndex: %s\n", err.Error())
		return nil, 0, err
	}

	s = dt.Value().(*model.Scalar)
	raw := s.DataType.Value()
	mtt.partitionColumnIdx = raw.(int32)

	dt, err = schema.Get("partitionSchema")
	if err != nil {
		fmt.Printf("Failed to get partitionSchema: %s\n", err.Error())
		return nil, 0, err
	}

	partitionSchema := dt.Value().(model.DataForm)

	dt, err = schema.Get("partitionType")
	if err != nil {
		fmt.Printf("Failed to get partitionType: %s\n", err.Error())
		return nil, 0, err
	}

	s = dt.Value().(*model.Scalar)
	raw = s.DataType.Value()
	return partitionSchema, raw.(int32), nil
}

func (mtt *MultiGoroutineTable) handlePartColNamesVector(partColNames model.DataForm, schema *model.Dictionary, partitionCol string) (model.DataForm, int32, error) {
	dims := partColNames.Rows()
	if dims > 1 && partitionCol == "" {
		return nil, 0, errors.New("the parameter partitionCol must be specified for a partitioned table")
	}

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
		return nil, 0, errors.New("the parameter partitionCol must be the partitioning columns in the partitioned table")
	}

	dt, err := schema.Get("partitionColumnIndex")
	if err != nil {
		fmt.Printf("Failed to get partitionColumnIndex: %s\n", err.Error())
		return nil, 0, err
	}

	s := dt.Value().(*model.Vector)
	raw := s.Data.ElementValue(ind)
	mtt.partitionColumnIdx = raw.(int32)

	dt, err = schema.Get("partitionSchema")
	if err != nil {
		fmt.Printf("Failed to get partitionSchema: %s\n", err.Error())
		return nil, 0, err
	}

	vct = dt.Value().(*model.Vector)
	partitionSchema := vct.Data.ElementValue(ind).(model.DataForm)

	dt, err = schema.Get("partitionType")
	if err != nil {
		fmt.Printf("Failed to get partitionType: %s\n", err.Error())
		return nil, 0, err
	}

	s = dt.Value().(*model.Vector)
	raw = s.Data.ElementValue(ind)
	return partitionSchema, raw.(int32), nil
}

func (mtt *MultiGoroutineTable) handlePartColNames(partColNames model.DataForm, schema *model.Dictionary, partitionCol string) (model.DataForm, int32, error) {
	if partColNames.GetDataForm() == model.DfScalar {
		return mtt.handlePartColNamesScalar(partColNames, schema, partitionCol)
	}

	return mtt.handlePartColNamesVector(partColNames, schema, partitionCol)
}
