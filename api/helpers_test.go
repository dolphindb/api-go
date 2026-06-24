package api

import (
	"testing"

	"github.com/dolphindb/api-go/v3/model"
)

func mustNewTable(t *testing.T, colNames []string, colValues []*model.Vector) *model.Table {
	t.Helper()
	tb, err := model.NewTable(colNames, colValues)
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func mustNewTableAppender(t *testing.T, opt *TableAppenderOption) *TableAppender {
	t.Helper()
	ta, err := NewTableAppender(opt)
	if err != nil {
		t.Fatal(err)
	}
	return ta
}
