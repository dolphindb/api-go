package domain

import (
	"testing"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestRangeDomain(t *testing.T) {
	dtl, err := model.NewDataTypeListFromRawData(model.DtString, []string{"domain", "sample", "zero"})
	assert.Nil(t, err)

	schema := model.NewVector(dtl)

	rd := &RangeDomain{
		rangeVector:  schema,
		dataTypeByte: model.DtString,
		category:     model.LITERAL,
	}

	dtl, err = model.NewDataTypeListFromRawData(model.DtBool, []byte{1})
	assert.Nil(t, err)

	schema = model.NewVector(dtl)

	_, err = rd.GetPartitionKeys(schema)
	assert.Equal(t, err.Error(), "data category incompatible")

	dtl, err = model.NewDataTypeListFromRawData(model.DtString, []string{"domain"})
	assert.Nil(t, err)

	schema = model.NewVector(dtl)

	keys, err := rd.GetPartitionKeys(schema)
	assert.Nil(t, err)
	assert.Equal(t, keys, []int{0})

	dtl, err = model.NewDataTypeListFromRawData(model.DtString, []string{"sample"})
	assert.Nil(t, err)

	schema = model.NewVector(dtl)

	keys, err = rd.GetPartitionKeys(schema)
	assert.Nil(t, err)
	assert.Equal(t, keys, []int{1})
}
