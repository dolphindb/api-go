package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequest(t *testing.T) {
	s := new(SaveTableRequest).SetDBPath("/db")
	assert.Equal(t, s.DBPath, "/db")

	l := new(LoadTextRequest).SetDelimiter(",")
	assert.Equal(t, l.Delimiter, ",")

	// st := new(SaveTextRequest).SetDelimiter(",")
	// assert.Equal(t, st.Delimiter, ",")

	pt := new(PloadTextRequest).SetDelimiter(",")
	assert.Equal(t, pt.Delimiter, ",")

	lt := new(LoadTableRequest).SetMemoryMode(true).
		SetPartitions("[test]")
	assert.Equal(t, lt.MemoryMode, true)
	assert.Equal(t, lt.Partitions, "[test]")

	ct := new(CreateTableRequest).SetSortColumns([]string{"id"})
	assert.Equal(t, ct.SortColumns, []string{"id"})

	tReq := new(TableRequest).SetTableParams([]TableParam{
		{
			Key:   "key",
			Value: "value",
		},
	})
	assert.Equal(t, tReq.TableParams[0], TableParam{
		Key:   "key",
		Value: "value",
	})
}
