package domain

import (
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestHashDomain(t *testing.T) {
	hd := &HashDomain{
		buckets: 10,
		dt:      model.DtDate,
		cat:     model.GetCategory(model.DtDate),
	}

	dtl, err := model.NewDataTypeListWithRaw(model.DtString, []string{"domain"})
	assert.Nil(t, err)

	pv := model.NewVector(dtl)
	_, err = hd.GetPartitionKeys(pv)
	assert.Equal(t, "data category incompatible", err.Error())

	dtl, err = model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, time.Month(1), 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	pv = model.NewVector(dtl)
	keys, err := hd.GetPartitionKeys(pv)
	assert.Nil(t, err)
	assert.Equal(t, keys[0], 3)
}
