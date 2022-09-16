package test

import (
	"context"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLoadTest(t *testing.T) {
	Convey("test_loadText_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		data := setup.DATADIR + "/TradesSmall.csv"
		Convey("test_loadText_filName_not_exist_exception", func() {
			loadT := new(api.LoadTextRequest).
				SetFileName("mssn.csv")
			_, err := ddb.LoadText(loadT)
			So(err, ShouldNotBeNil)
		})
		Convey("test_loadText_para_filename", func() {
			tmp, err := ddb.RunScript("select * from loadText(\"" + data + "\")")
			ex := tmp.(*model.Table)
			So(err, ShouldBeNil)
			re, err := LoadTextFileName(ddb, data)
			So(err, ShouldBeNil)
			result := CompareTablesDataformTable(ex, re)
			So(result, ShouldBeTrue)
		})
		Convey("test_loadText_para_delimiter", func() {
			tmp, err := ddb.RunScript("select * from loadText(\"" + data + "\", ';')")
			ex := tmp.(*model.Table)
			So(err, ShouldBeNil)
			re, err := LoadTextDelimiter(ddb, data, ";")
			So(err, ShouldBeNil)
			result := CompareTablesDataformTable(ex, re)
			So(result, ShouldBeTrue)
		})
		So(ddb.Close(), ShouldBeNil)
	})
}
