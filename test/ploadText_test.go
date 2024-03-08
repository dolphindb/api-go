package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var host15 = getRandomClusterAddress()

func TestPloadTest(t *testing.T) {
	t.Parallel()
	Convey("test_PloadTest_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host15, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		data := setup.DATA_DIR + "/TradesSmall.csv"
		fmt.Println(data)
		Convey("test_ploadText_ex1", func() {
			_, err = ddb.RunScript(fmt.Sprintf(`%s=ploadText("%s","%s")`, "m", "''", ","))
			So(err, ShouldNotBeNil)
		})
		Convey("test_loadTest_para_filename", func() {
			tmp, err := ddb.RunScript("select * from loadText(\"" + data + "\")")
			ex := tmp.(*model.Table)
			So(err, ShouldBeNil)
			re, err := PloadTextFileName(ddb, data)
			So(err, ShouldBeNil)
			result := CompareTablesDataformTable(ex, re)
			So(result, ShouldBeTrue)
		})
		Convey("test_PloadTest_para_filename", func() {
			tmp, err := ddb.RunScript("select * from ploadText(\"" + data + "\")")
			ex := tmp.(*model.Table)
			So(err, ShouldBeNil)
			re, err := PloadTextFileName(ddb, data)
			So(err, ShouldBeNil)
			result := CompareTablesDataformTable(ex, re)
			So(result, ShouldBeTrue)
		})
		Convey("test_PloadTest_para_delimiter", func() {
			tmp, err := ddb.RunScript("select * from loadText(\"" + data + "\", ';')")
			ex := tmp.(*model.Table)
			So(err, ShouldBeNil)
			re, err := PloadTextDelimiter(ddb, data, ";")
			So(err, ShouldBeNil)
			result := CompareTablesDataformTable(ex, re)
			So(result, ShouldBeTrue)
		})
	})
}
