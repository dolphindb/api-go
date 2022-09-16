package domain

import (
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestValueDomain(t *testing.T) {
	vd := &ValueDomain{
		dt:  model.DtDate,
		cat: model.TEMPORAL,
	}

	dtl, err := model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, time.Month(1), 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	pc := model.NewVector(dtl)
	keys, err := vd.GetPartitionKeys(pc)
	assert.Nil(t, err)
	assert.Equal(t, keys, []int{18993})
}
