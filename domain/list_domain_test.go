package domain

import (
	"testing"
	"time"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestListDomain(t *testing.T) {
	dt, err := model.NewDataType(model.DtDate, time.Date(2022, time.Month(1), 1, 1, 1, 1, 1, time.UTC))
	assert.Nil(t, err)

	val := model.NewScalar(dt)

	dtl, err := model.NewDataTypeListWithRaw(model.DtAny, []model.DataForm{val})
	assert.Nil(t, err)

	vct := model.NewVector(dtl)

	ld, err := NewListDomain(vct, model.DtDate, model.TEMPORAL)
	assert.Nil(t, err)

	dtl, err = model.NewDataTypeListWithRaw(model.DtDatetime, []time.Time{time.Date(2022, time.Month(1), 1, 1, 1, 1, 1, time.UTC)})
	assert.Nil(t, err)

	pv := model.NewVector(dtl)
	keys, err := ld.GetPartitionKeys(pv)
	assert.Nil(t, err)
	assert.Equal(t, keys, []int{0})
}
