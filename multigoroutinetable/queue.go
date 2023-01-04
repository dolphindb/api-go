package multigoroutinetable

import (
	"sync"

	"github.com/dolphindb/api-go/model"
)

type queue struct {
	buf  [][]model.DataType
	l    int
	lock sync.RWMutex
}

func newQueue(size int) *queue {
	return &queue{
		buf:  make([][]model.DataType, 0, size),
		lock: sync.RWMutex{},
	}
}

func (q *queue) add(in []model.DataType) {
	q.lock.Lock()
	q.buf = append(q.buf, in)
	q.l++
	q.lock.Unlock()
}

func (q *queue) load() []model.DataType {
	if q.len() == 0 {
		return nil
	}
	q.lock.Lock()
	res := q.buf[0]
	q.buf = q.buf[1:]
	q.l--
	q.lock.Unlock()
	return res
}

func (q *queue) len() int {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.l
}
