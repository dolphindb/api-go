package streaming

import (
	"testing"

	"github.com/dolphindb/api-go/model"

	"github.com/stretchr/testify/assert"
)

func TestMessage(t *testing.T) {
	dtl, err := model.NewDataTypeListFromRawData(model.DtString, []string{"name", "prefix"})
	assert.Nil(t, err)

	s0 := model.NewScalar(dtl.Get(0))
	s1 := model.NewScalar(dtl.Get(1))

	dtl, err = model.NewDataTypeListFromRawData(model.DtAny, []model.DataForm{s0, s1})
	assert.Nil(t, err)

	vct := model.NewVector(dtl)

	msg := &Message{
		offset: -1,
		topic:  "topic",
		nameToIndex: map[string]int{
			"name":   0,
			"prefix": 1,
		},
		msg: vct,
	}

	assert.Equal(t, msg.GetOffset(), int64(-1))
	assert.Equal(t, msg.GetTopic(), "topic")
	assert.Equal(t, msg.GetValue(0), s0)
	assert.Equal(t, msg.GetValue(1), s1)
	assert.Equal(t, msg.GetValueByName("name"), s0)
	assert.Equal(t, msg.GetValueByName("prefix"), s1)
}
