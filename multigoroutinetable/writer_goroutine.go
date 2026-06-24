package multigoroutinetable

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/model"
)

type writerGoroutine struct {
	dialer.Conn

	signal      *sync.Cond
	tableWriter *MultiGoroutineTable
	writeQueue  *queue
	failedQueue *queue

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
		signal:         sync.NewCond(&sync.Mutex{}),
		exit:           make(chan bool),
		writeQueue:     newQueue(mtw.batchSize, mtw),
		failedQueue:    newQueue(mtw.batchSize, mtw),
	}

	res.writeQueue.initBuf()
	res.failedQueue.initBuf()

	res.initScript()

	go res.run()

	time.Sleep(1 * time.Millisecond)

	return res
}

func (w *writerGoroutine) run() {
	w.exit = make(chan bool)

	for !w.isExit() {
		if w.writeQueue.len() < w.tableWriter.batchSize {
			time.Sleep(time.Duration(w.tableWriter.throttle) * time.Millisecond)
		}
		w.writeAllData()
	}

	for !w.tableWriter.isExit() && w.writeAllData() {
	}

	w.isFinished = true
}

func (w *writerGoroutine) getStatus(status *GoroutineStatus) {
	status.GoroutineIndex = w.goroutineIndex
	status.SentRows = w.sentRows
	status.UnSentRows = w.writeQueue.len()
	status.UnsentRows = w.writeQueue.len()
	status.FailedRows = w.failedQueue.len()
}

func (w *writerGoroutine) initScript() {
	if w.tableWriter.database == "" {
		w.insertScript = fmt.Sprintf("tableInsert{\"%s\"}", w.tableWriter.tableName)
	} else {
		w.insertScript = fmt.Sprintf("tableInsert{loadTable(\"%s\",\"%s\")}", w.tableWriter.database, w.tableWriter.tableName)
	}
}

func (w *writerGoroutine) writeAllData() bool {
	items := w.writeQueue.popAll()

	if size := len(items); size < 1 {
		return false
	}

	defer w.handlePanic(items)

	for _, v := range items {
		isWriteDone := true
		writeTable, addRowCount, newItems, err := w.generateWriteTableFromInterface(v)
		if err != nil {
			isWriteDone = false
			w.handleError(err.Error())
		}

		if writeTable != nil && addRowCount > 0 {
			err := w.runScript(writeTable, addRowCount)
			if err != nil {
				isWriteDone = false
				w.handleError(err.Error())
			}
		}
		select {
		case w.writeQueue.bufPool <- newItems:
		default:
		}

		if !isWriteDone {
			if addRowCount < 0 {
				addRowCount = 0
			}
			w.failedQueue.addBatch(v, addRowCount)
		}
	}

	return true
}

func (w *writerGoroutine) handlePanic(items [][]interface{}) {
	re := recover()
	if re != nil {
		for _, v := range items {
			w.failedQueue.addBatch(v, 0) // FIXME
		}

		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		multiGoroutineTableLogErrorf("failed to insert data into the table: %s", string(buf[:n]))
		w.handleError(string(buf))
	}
}

func (w *writerGoroutine) handleError(errMsg string) {
	w.tableWriter.errorInfo = errMsg
	w.tableWriter.hasError = true
	if w.Conn != nil {
		w.Conn.Close()
	}

	w.Conn = nil
}

func (w *writerGoroutine) generateTableCols(items [][]model.DataType) []*model.Vector {
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
			vct, _ = model.NewVectorWithArrayVector(av)
		default:
			dtl := model.NewEmptyDataTypeList(model.DataTypeByte(v), len(items))
			vct = model.NewVector(dtl)
		}

		colValues[k] = vct
	}

	return colValues
}

func (w *writerGoroutine) generateWriteTableFromInterface(items []interface{}) (*model.Table, int, []interface{}, error) {
	count := -1
	colValues := make([]*model.Vector, len(w.tableWriter.colTypes))
	newItems := make([]interface{}, len(items))
	for ind, dtValue := range w.tableWriter.colTypes {
		prepared, err := prepareBatchColumn(model.DataTypeByte(dtValue), items[ind])
		if err != nil {
			return nil, -1, nil, fmt.Errorf("col %d of type %s: %w", ind, model.GetDataTypeString(model.DataTypeByte(dtValue)), err)
		}

		if count == -1 {
			count = prepared.count
		} else if prepared.count != count {
			return nil, -1, nil, fmt.Errorf("column batch sizes don't match: expect %d, got %d for col %d", count, prepared.count, ind)
		}

		colValues[ind] = prepared.vector
		newItems[ind] = prepared.empty
	}
	items = nil

	if count < 0 {
		count = 0
	}

	tb, err := model.NewTable(w.tableWriter.colNames, colValues)
	if err != nil {
		return nil, -1, nil, err
	}

	return tb, count, newItems, nil
}

func (w *writerGoroutine) generateWriteTable(items [][]model.DataType) (*model.Table, bool) {
	isWriteDone := true
	colValues := w.generateTableCols(items)

	for k, row := range items {
		for ind, col := range colValues {
			if col.ArrayVector == nil {
				err := col.Set(k, row[ind])
				if err != nil {
					multiGoroutineTableLogErrorf("failed to set DataType into Vector: %v", err)
					isWriteDone = false
					w.handleError(err.Error())
					break
				}
			}
		}
	}

	if isWriteDone {
		tb, err := model.NewTable(w.tableWriter.colNames, colValues)
		if err == nil {
			return tb, true
		}
		multiGoroutineTableLogErrorf("failed to build table from vectors: %v", err)
		w.handleError(err.Error())
	}

	return nil, false
}

func (w *writerGoroutine) runScript(df model.DataForm, count int) error {
	args := make([]model.DataForm, 1)
	args[0] = df
	_, err := w.RunFunc(w.insertScript, args)
	if err != nil {
		multiGoroutineTableLogErrorf("failed to run func: %v", err)
		return err
	}

	if w.saveScript != "" {
		_, err = w.RunScript(w.saveScript)
		if err != nil {
			multiGoroutineTableLogErrorf("failed to run script: %v", err)
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
	close(w.exit)

	w.signal.Signal()
}
