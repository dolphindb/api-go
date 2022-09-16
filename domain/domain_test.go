package domain

import (
	"testing"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestDomain(t *testing.T) {
	dtb := model.DtString

	dtl, err := model.NewDataTypeListWithRaw(dtb, []string{"domain"})
	assert.Nil(t, err)

	schema := model.NewVector(dtl)

	pt := GetPartitionType(0)
	_, err = CreateDomain(pt, dtb, schema)
	assert.Equal(t, "unsupported partition type SEQ", err.Error())

	pt = GetPartitionType(1)
	domain, err := CreateDomain(pt, dtb, schema)
	assert.Nil(t, err)

	vd := domain.(*ValueDomain)
	assert.Equal(t, vd.dt, dtb)
	assert.Equal(t, vd.cat, model.GetCategory(dtb))

	pt = GetPartitionType(2)
	domain, err = CreateDomain(pt, dtb, schema)
	assert.Nil(t, err)

	rd := domain.(*RangeDomain)
	assert.Equal(t, rd.dt, dtb)
	assert.Equal(t, rd.cat, model.GetCategory(dtb))

	dtl, err = model.NewDataTypeListWithRaw(model.DtAny, []model.DataForm{schema})
	assert.Nil(t, err)

	schema = model.NewVector(dtl)

	pt = GetPartitionType(3)
	domain, err = CreateDomain(pt, dtb, schema)
	assert.Nil(t, err)

	ld := domain.(*ListDomain)
	assert.Equal(t, ld.dt, dtb)
	assert.Equal(t, ld.cat, model.GetCategory(dtb))

	pt = GetPartitionType(4)
	_, err = CreateDomain(pt, dtb, schema)
	assert.Equal(t, "unsupported partition type COMPO", err.Error())

	dt, err := model.NewDataType(model.DtInt, int32(10))
	assert.Nil(t, err)

	sca := model.NewScalar(dt)
	pt = GetPartitionType(5)
	domain, err = CreateDomain(pt, dtb, sca)
	assert.Nil(t, err)

	hd := domain.(*HashDomain)
	assert.Equal(t, hd.dt, dtb)
	assert.Equal(t, hd.cat, model.GetCategory(dtb))

	pt = GetPartitionType(6)
	_, err = CreateDomain(pt, dtb, schema)
	assert.Equal(t, "unsupported partition type SEQ", err.Error())
}
