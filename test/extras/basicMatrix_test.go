package test

import (
	"testing"

	"github.com/dolphindb/api-go/model"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Matrix(t *testing.T) {
	Convey("Test_matrix", t, func() {
		data, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
		So(err, ShouldBeNil)

		rl, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
		So(err, ShouldBeNil)
		cl, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1})
		So(err, ShouldBeNil)
		mtx := model.NewMatrix(model.NewVector(data), nil, nil)

		mtx.Data = nil
		So(mtx.String(), ShouldEqual, "")

		mtx.Data = model.NewVector(data)
		mtx.Data.Data = nil
		So(mtx.String(), ShouldEqual, "matrix<int>[5r][1c]({\n  rows: null,\n  cols: null,\n  data: null,\n})")

		mtx.RowLabels = model.NewVector(rl)
		mtx.RowLabels.Data = nil
		mtx.ColumnLabels = model.NewVector(cl)
		mtx.ColumnLabels.Data = nil
		So(mtx.String(), ShouldEqual, "matrix<int>[5r][1c]({\n  rows: null,\n  cols: null,\n  data: null,\n})")
	})
}
