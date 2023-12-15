package test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Chart_DownLoad_DataType(t *testing.T) {
	t.Parallel()
	Convey("Test_Chart:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_chart_plot:", func() {
			s, err := db.RunScript("x=1*(1..5);t=table(x);plot(t,x)")
			So(err, ShouldBeNil)
			result := s.(*model.Chart)
			re := result.String()
			So(re, ShouldNotBeNil)
			reType := result.GetDataType()
			So(reType, ShouldEqual, 25)
			reTypeString := result.GetDataTypeString()
			So(reTypeString, ShouldEqual, "any")
			form := result.GetDataForm()
			So(form, ShouldEqual, model.DfChart)
			row := result.Rows()
			So(row, ShouldEqual, model.DtInt)
			title := result.GetTitle()
			So(title, ShouldEqual, "")
			ctype := result.GetChartType()
			So(ctype, ShouldEqual, "CT_LINE")
			xna := result.GetXAxisName()
			yna := result.GetYAxisName()
			So(xna, ShouldEqual, "x")
			So(yna, ShouldEqual, "")
			by := bytes.NewBufferString("")
			w := protocol.NewWriter(by)
			err = result.Render(w, protocol.LittleEndian)
			So(err, ShouldBeNil)
			w.Flush()
			by.Reset()
		})
		So(db.Close(), ShouldBeNil)
	})
}
func Test_Chart_UpLoad_DataType(t *testing.T) {
	t.Parallel()
	Convey("Test_Chart_upload:", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_chart_upload:", func() {
			dtl, err := model.NewDataTypeListFromRawData(model.DtString, []string{"chart", "xaxis", "yaxis"})
			So(err, ShouldBeNil)
			So(dtl.DataType(), ShouldEqual, model.DtString)
			dl, err := model.NewDataTypeListFromRawData(model.DtString, []string{"chart", "xaxis", "yaxis"})
			So(err, ShouldBeNil)
			ti := model.NewVector(dl)
			dt, err := model.NewDataType(model.DtInt, int32(4))
			So(err, ShouldBeNil)
			ct := model.NewScalar(dt)
			dt, err = model.NewDataType(model.DtBool, byte(0))
			So(err, ShouldBeNil)
			st := model.NewScalar(dt)
			d, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
			So(err, ShouldBeNil)
			rl, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5})
			So(err, ShouldBeNil)
			cl, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1})
			So(err, ShouldBeNil)
			data := model.NewMatrix(model.NewVector(d), model.NewVector(rl), model.NewVector(cl))
			ch := model.NewChart(map[string]model.DataForm{
				"title":     ti,
				"chartType": ct,
				"stacking":  st,
				"data":      data,
			})
			fmt.Print(ch)
			So(ch.GetDataForm(), ShouldEqual, model.DfChart)
			So(ch.GetDataType(), ShouldEqual, model.DtAny)
			So(ch.GetDataTypeString(), ShouldEqual, "any")
			So(ch.GetTitle(), ShouldEqual, "chart")
			So(ch.GetXAxisName(), ShouldEqual, "xaxis")
			So(ch.GetYAxisName(), ShouldEqual, "yaxis")
			So(ch.GetChartType(), ShouldEqual, "CT_LINE")
			_, err = db.Upload(map[string]model.DataForm{"s": ch})
			res, _ := db.RunScript("s")
			ty, _ := db.RunScript("typestr(s)")
			fmt.Print(res)
			re := res.(*model.Dictionary).Values
			So(re.Get(3).String(), ShouldEqual, data.String())
			So(re.Get(2).String(), ShouldEqual, st.String())
			So(re.Get(1).String(), ShouldEqual, ct.String())
			So(re.Get(0).String(), ShouldEqual, ti.String())
			fmt.Print(re)
			So(err, ShouldBeNil)
			So(ty.String(), ShouldEqual, "string(STRING->ANY DICTIONARY)")
			So(res.GetDataType(), ShouldEqual, model.DtAny)
		})
		So(db.Close(), ShouldBeNil)
	})
}
