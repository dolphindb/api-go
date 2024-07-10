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
	batch := 65535
	if mtw.batchSize > batch {
		batch = mtw.batchSize
	}
	res := &writerGoroutine{
		goroutineIndex: goroutineIndex,
		Conn:           conn,
		tableWriter:    mtw,
		signal:         sync.NewCond(&sync.Mutex{}),
		exit:           make(chan bool),
		writeQueue:     newQueue(batch, mtw),
		failedQueue:    newQueue(batch, mtw),
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
			for !w.isExit() {
				if w.writeQueue.len() < w.tableWriter.batchSize {
					time.Sleep(time.Duration(w.tableWriter.throttle) * time.Millisecond)
				}
				w.writeAllData()
			}
		}
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
		writeTable, addRowCount, newItems := w.generateWriteTableFromInterface(v)
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

func (w *writerGoroutine) generateWriteTableFromInterface(items []interface{}) (*model.Table, int, []interface{}) {
	count := 0
	// for column
	colValues := make([]*model.Vector, len(w.tableWriter.colTypes))
	newItems := make([]interface{}, len(items))
	for ind, dtValue := range w.tableWriter.colTypes {
		var vct *model.Vector
		var dtl model.DataTypeList
		var err error
		dt := model.DataTypeByte(dtValue)
		switch {
		case dt >= 128:
			//FIXME
			dtl := model.NewEmptyDataTypeList(model.DataTypeByte(dt-128), len(items))
			vct = model.NewVector(dtl)
		case dt >= 64:
			vl := make([]*model.Vector, 0)
			vec := items[ind].([]model.DataType)

			for i := 0; i < len(vec); i++ {
				item := vec[i].Value().(*model.Vector)
				vl = append(vl, item)
			}
			av := model.NewArrayVector(vl)
			vct = model.NewVectorWithArrayVector(av)
			count = len(vec)
			newItems[ind] = vec[:0]
		case dt == model.DtBool:
			vec := items[ind].([]byte)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtBlob:
			vec := items[ind].([][]byte)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtChar || dt == model.DtCompress:
			vec := items[ind].([]byte)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtComplex || dt == model.DtPoint:
			vec := items[ind].([][2]float64)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case (dt >= model.DtDate && dt <= model.DtNanoTimestamp) || (dt >= model.DtDateHour && dt <= model.DtDateMinute):
			vec := items[ind].([]time.Time)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtShort:
			vec := items[ind].([]int16)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtInt:
			vec := items[ind].([]int32)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtLong:
			vec := items[ind].([]int64)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtFloat:
			vec := items[ind].([]float32)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtDouble:
			vec := items[ind].([]float64)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]

		case dt == model.DtDecimal32:
			vec := items[ind].([]*model.Decimal32)
			count = len(vec)
			dtl = model.NewEmptyDataTypeList(dt, count)
			for ind, v := range vec {
				err := dtl.SetWithRawData(ind, v)
				if err != nil {
					w.handleError(err.Error())
					return nil, -1, nil
				}
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtDecimal64:
			vec := items[ind].([]*model.Decimal64)
			count = len(vec)
			dtl = model.NewEmptyDataTypeList(dt, count)
			for ind, v := range vec {
				err := dtl.SetWithRawData(ind, v)
				if err != nil {
					w.handleError(err.Error())
					return nil, -1, nil
				}
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtDecimal128:
			vec := items[ind].([]*model.Decimal128)
			count = len(vec)
			dtl = model.NewEmptyDataTypeList(dt, count)
			for ind, v := range vec {
				err := dtl.SetWithRawData(ind, v)
				if err != nil {
					w.handleError(err.Error())
					return nil, -1, nil
				}
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]

		case dt == model.DtUUID || dt == model.DtString || dt == model.DtSymbol || dt == model.DtDuration || dt == model.DtInt128 || dt == model.DtIP:
			vec := items[ind].([]string)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		case dt == model.DtAny:
			vec := items[ind].([]model.DataForm)
			count = len(vec)
			dtl, err = model.NewDataTypeListFromRawData(dt, vec)
			if err != nil {
				w.handleError(err.Error())
				return nil, -1, nil
			}
			vct = model.NewVector(dtl)
			newItems[ind] = vec[:0]
		default:
			return nil, -1, nil
		}
		colValues[ind] = vct
	}
	items = nil

	return model.NewTable(w.tableWriter.colNames, colValues), count, newItems
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
