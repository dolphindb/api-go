package multigoroutinetable

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
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
		writeQueue:     newQueue(mtw.batchSize),
		failedQueue:    newQueue(mtw.batchSize),
	}

	res.initScript()

	go res.run()

	time.Sleep(1 * time.Millisecond)

	return res
}

func (w *writerGoroutine) run() {
	w.exit = make(chan bool)

	for !w.isExit() {
		w.signal.L.Lock()
		w.signal.Wait()
		w.signal.L.Unlock()
		if !w.isExit() && w.tableWriter.batchSize > 1 && w.tableWriter.throttle > 0 {
			if !w.isExit() && w.writeQueue.len() < w.tableWriter.batchSize {
				time.Sleep(time.Duration(w.tableWriter.throttle) * time.Millisecond)
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
	items := make([][]model.DataType, 0)
	for w.writeQueue.len() > 0 {
		if val := w.writeQueue.load(); val != nil {
			items = append(items, val)
		}
	}

	if size := len(items); size < 1 {
		return false
	}

	defer w.handlePanic(items)

	addRowCount := len(items)
	writeTable, isWriteDone := w.generateWriteTable(items)
	if isWriteDone && writeTable != nil && addRowCount > 0 {
		err := w.runScript(writeTable, addRowCount)
		if err != nil {
			isWriteDone = false
			w.handleError(err.Error())
		}
	}

	if !isWriteDone {
		for _, v := range items {
			w.failedQueue.add(v)
		}
	}

	return true
}

func (w *writerGoroutine) handlePanic(items [][]model.DataType) {
	re := recover()
	if re != nil {
		for _, v := range items {
			w.failedQueue.add(v)
		}

		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		fmt.Println("Failed to insert data into the table: ", string(buf[:n]))
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
			vct = model.NewVectorWithArrayVector(av)
		default:
			dtl := model.NewEmptyDataTypeList(model.DataTypeByte(v), len(items))
			vct = model.NewVector(dtl)
		}

		colValues[k] = vct
	}

	return colValues
}

func (w *writerGoroutine) generateWriteTable(items [][]model.DataType) (*model.Table, bool) {
	isWriteDone := true
	colValues := w.generateTableCols(items)

	for k, row := range items {
		for ind, col := range colValues {
			if col.ArrayVector == nil {
				err := col.Set(k, row[ind])
				if err != nil {
					fmt.Println("Failed to set DataType into Vector: ", err)
					isWriteDone = false
					w.handleError(err.Error())
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

func (w *writerGoroutine) runScript(df model.DataForm, count int) error {
	args := make([]model.DataForm, 1)
	args[0] = df
	_, err := w.RunFunc(w.insertScript, args)
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
	close(w.exit)

	w.signal.Signal()
}
