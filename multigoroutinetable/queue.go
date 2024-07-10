package multigoroutinetable

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/dolphindb/api-go/model"
)

type queue struct {
	buf         [][]interface{}
	bufPool     chan []interface{}
	l           int
	lock        sync.RWMutex
	tableWriter *MultiGoroutineTable
	lastLength  int
}

func newQueue(size int, tableWriter *MultiGoroutineTable) *queue {
	return &queue{
		buf:         make([][]interface{}, 0, size),
		bufPool:     make(chan []interface{}, 10000),
		lock:        sync.RWMutex{},
		tableWriter: tableWriter,
	}
}

// every interface is a slice of basic type
func (q *queue) addBatch(in []interface{}, length int) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.buf = append(q.buf, in)
	q.l += length
}

func (q *queue) makeQueueBuf(colTypes []int, batchSize int) []interface{} {
	select {
	case buf := <-q.bufPool:
		return buf
	default:
		break
	}
	queueBuf := make([]interface{}, len(colTypes))
	for k, v := range colTypes {
		switch model.DataTypeByte(v) {
		case model.DtBool:
			queueBuf[k] = make([]byte, 0, batchSize)
		case model.DtBlob:
			queueBuf[k] = make([][]byte, 0, batchSize)
		case model.DtChar, model.DtCompress:
			queueBuf[k] = make([]byte, 0, batchSize)
		case model.DtComplex, model.DtPoint:
			queueBuf[k] = make([][2]float64, 0, batchSize)
		case model.DtShort:
			queueBuf[k] = make([]int16, 0, batchSize)
		case model.DtInt:
			queueBuf[k] = make([]int32, 0, batchSize)
		case model.DtLong:
			queueBuf[k] = make([]int64, 0, batchSize)
		case model.DtFloat:
			queueBuf[k] = make([]float32, 0, batchSize)
		case model.DtDouble:
			queueBuf[k] = make([]float64, 0, batchSize)
		case model.DtDecimal32:
			queueBuf[k] = make([]*model.Decimal32, 0, batchSize)
		case model.DtDecimal64:
			queueBuf[k] = make([]*model.Decimal64, 0, batchSize)
		case model.DtDecimal128:
			queueBuf[k] = make([]*model.Decimal128, 0, batchSize)
		case model.DtDate, model.DtDateHour, model.DtDateMinute, model.DtDatetime, model.DtMinute, model.DtMonth, model.DtNanoTime, model.DtSecond, model.DtTime, model.DtTimestamp, model.DtNanoTimestamp:
			queueBuf[k] = make([]time.Time, 0, batchSize)
		case model.DtUUID, model.DtSymbol, model.DtString, model.DtDuration, model.DtInt128, model.DtIP:
			queueBuf[k] = make([]string, 0, batchSize)
		case model.DtAny:
			queueBuf[k] = make([]model.DataForm, 0, batchSize)
		default:
			// HACK other type should be DataType
			queueBuf[k] = make([]model.DataType, 0, batchSize)
		}
	}
	return queueBuf
}

// every interface is a basic type
func (q *queue) add(in []interface{}) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	batch := 65535
	if q.tableWriter.batchSize > batch {
		batch = q.tableWriter.batchSize
	}
	if len(q.buf) == 0 {
		q.buf = append(q.buf, q.makeQueueBuf(q.tableWriter.colTypes, batch))
		q.lastLength = 0
	} else if q.lastLength == batch {
		q.buf = append(q.buf, q.makeQueueBuf(q.tableWriter.colTypes, batch))
		q.lastLength = 0
	}

	for ind, v := range in {
		dt := model.DataTypeByte(q.tableWriter.colTypes[ind])
		if dt > 128 {
			// TODO don't know the usage of type greater than 128
			continue
		} else if dt > 64 {
			var val model.DataType
			var err error
			if v == nil {
				dtl := model.NewEmptyDataTypeList(dt, 1)
				vct := model.NewVector(dtl)
				val, err = model.NewDataType(model.DtAny, vct)
				if err != nil {
					return err
				}
			} else {
				dtl, err := model.NewDataTypeListFromRawData(dt, v)
				if err != nil {
					return err
				}

				if dtl.Len() == 0 {
					dtl = model.NewEmptyDataTypeList(dt, 1)
				}

				vct := model.NewVector(dtl)
				val, err = model.NewDataType(model.DtAny, vct)
				if err != nil {
					return err
				}
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]model.DataType), val)
			continue
		}
		switch dt {
		case model.DtBool:
			if v == nil {
				v = model.NullBool
			}
			var val byte
			switch value := v.(type) {
			case byte:
				val = v.(byte)
			case bool:
				if value {
					val = 1
				} else {
					val = 0
				}
			default:
				return errors.New("the type of in must be bool when datatype is DtBool")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]byte), val)
		case model.DtBlob:
			if v == nil {
				v = model.NullBlob
			}
			val, ok := v.([]byte)
			if !ok {
				return errors.New("the type of in must be []byte when datatype is DtBlob")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([][]byte), val)
		case model.DtChar, model.DtCompress:
			if v == nil {
				v = model.NullBool
			}
			val, ok := v.(byte)
			if !ok {
				return errors.New("the type of in must be byte when datatype is DtChar or DtCompress")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]byte), val)
		case model.DtComplex, model.DtPoint:
			if v == nil {
				var value [2]float64
				value[0] = -math.MaxFloat64
				value[1] = -math.MaxFloat64
				v = value
			}
			val, ok := v.([2]float64)
			if !ok {
				return errors.New("the type of in must be [2]float64 when datatype is DtComplex or DtPoint")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([][2]float64), val)
		case model.DtShort:
			if v == nil {
				v = model.NullShort
			}
			val, ok := v.(int16)
			if !ok {
				return errors.New("the type of in must be int16 when datatype is DtShort")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]int16), val)
		case model.DtInt:
			if v == nil {
				v = model.NullInt
			}
			val, ok := v.(int32)
			if !ok {
				return errors.New("the type of in must be int32 when datatype is DtInt")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]int32), val)
		case model.DtLong:
			if v == nil {
				v = model.NullLong
			}
			val, ok := v.(int64)
			if !ok {
				return errors.New("the type of in must be int64 when datatype is DtLong")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]int64), val)
		case model.DtFloat:
			if v == nil {
				v = model.NullFloat
			}
			val, ok := v.(float32)
			if !ok {
				return errors.New("the type of in must be float32 when datatype is DtFloat")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]float32), val)
		case model.DtDouble:
			if v == nil {
				v = model.NullDouble
			}
			val, ok := v.(float64)
			if !ok {
				return errors.New("the type of in must be float64 when datatype is DtDouble")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]float64), val)
		case model.DtDecimal32:
			if v == nil {
				value := &model.Decimal32{Scale: 6, Value: model.NullDecimal32Value}
				v = value
			}
			val, ok := v.(*model.Decimal32)
			if !ok {
				return errors.New("the type of in must be *model.Decimal32 when datatype is DtDecimal32")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]*model.Decimal32), val)
		case model.DtDecimal64:
			if v == nil {
				value := &model.Decimal64{Scale: 6, Value: model.NullDecimal64Value}
				v = value
			}
			val, ok := v.(*model.Decimal64)
			if !ok {
				return errors.New("the type of in must be *model.Decimal64 when datatype is DtDecimal64")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]*model.Decimal64), val)
		case model.DtDecimal128:
			if v == nil {
				value := &model.Decimal128{Scale: 6, Value: model.NullDecimal128Value}
				v = value
			}
			val, ok := v.(*model.Decimal128)
			if !ok {
				return errors.New("the type of in must be *model.Decimal128 when datatype is DtDecimal128")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]*model.Decimal128), val)
		case model.DtDate, model.DtDateHour, model.DtDateMinute, model.DtDatetime, model.DtMinute, model.DtMonth, model.DtNanoTime, model.DtSecond, model.DtTime, model.DtTimestamp, model.DtNanoTimestamp:
			if v == nil {
				v = model.NullTime
			}
			val, ok := v.(time.Time)
			if !ok {
				return errors.New("the type of in must be time.Time when datatype is " + model.GetDataTypeString(dt))
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]time.Time), val)
		case model.DtUUID, model.DtSymbol, model.DtString, model.DtDuration, model.DtInt128, model.DtIP:
			if v == nil {
				v = model.NullString
			}
			val, ok := v.(string)
			if !ok {
				return errors.New("the type of in must be string when datatype is " + model.GetDataTypeString(dt))
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]string), val)
		case model.DtAny:
			if v == nil {
				v = model.NullAny
			}
			val, ok := v.(model.DataForm)
			if !ok {
				return errors.New("the type of in must be model.DataForm when datatype is DtAny")
			}
			q.buf[len(q.buf)-1][ind] = append(q.buf[len(q.buf)-1][ind].([]model.DataForm), val)

		default:
			return fmt.Errorf("invalid DataType %d", dt)
		}
	}
	q.lastLength++
	q.l++
	return nil
}

func (q *queue) popAll() [][]interface{} {
	if len(q.buf) == 0 {
		return nil
	}
	q.lock.Lock()
	defer q.lock.Unlock()
	ret := make([][]interface{}, len(q.buf))
	copy(ret, q.buf)
	q.buf = make([][]interface{}, 0, 32)
	q.l = 0
	q.lastLength = 0
	return ret
}

func (q *queue) len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.l
}
