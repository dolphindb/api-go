package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Chart(t *testing.T) {
	Convey("Test_Chart_withExtras", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)

		titleDt, err := model.NewDataType(model.DtString, "title")
		So(err, ShouldBeNil)
		title := model.NewScalar(titleDt)

		dtKey, err := model.NewDataTypeListFromRawData(model.DtString, []string{"key1", "key2"})
		So(err, ShouldBeNil)
		keys := model.NewVector(dtKey)

		dtVal, err := model.NewDataTypeListFromRawData(model.DtString, []string{"val1", "val2"})
		So(err, ShouldBeNil)
		vals := model.NewVector(dtVal)
		extras := model.NewDictionary(keys, vals)
		ch := model.NewChart(map[string]model.DataForm{"extras": extras, "notin": extras})

		ch.Title = title
		So(ch.GetTitle(), ShouldEqual, "title")
		So(ch.GetChartType(), ShouldEqual, "")
		So(ch.GetXAxisName(), ShouldEqual, "")
		So(ch.GetYAxisName(), ShouldEqual, "")
		So(ch.String(), ShouldNotBeNil)

		ch.Title = nil
		So(ch.GetTitle(), ShouldEqual, "")
		So(ch.GetXAxisName(), ShouldEqual, "")
		So(ch.GetYAxisName(), ShouldEqual, "")
		So(ch.String(), ShouldNotBeNil)

		_, err = db.Upload(map[string]model.DataForm{"ch": ch})
		So(err, ShouldBeNil)

		t, err := model.NewDataTypeListFromRawData(model.DtString, []string{"title"})
		So(err, ShouldBeNil)
		ch.Title = model.NewVector(t)
		So(ch.GetXAxisName(), ShouldEqual, "")
		So(ch.GetYAxisName(), ShouldEqual, "")

		ch.Title = ch
		ch.Stacking = model.NullAny
		So(ch.String(), ShouldNotBeEmpty)
	})
}
