package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestSql(t *testing.T) {
	t.Parallel()
	Convey("Test_sql", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test_sql_select_NULL", func() {
			Convey("Test_sql_select_NULL_with_table_data", func() {
				var sql string = "select *, NULL from table(1..100 as id)"
				tab, err := ddb.RunScript(sql)
				So(err, ShouldBeNil)
				ex_col0, _ := ddb.RunScript("table(1..100 as id)[`id]")
				So(tab.(*model.Table).Columns(), ShouldEqual, 2)
				So(tab.(*model.Table).Rows(), ShouldEqual, ex_col0.(*model.Vector).Rows())
				So(tab.(*model.Table).GetColumnByIndex(1).GetDataType(), ShouldEqual, model.DtVoid)
				for i := 0; i < ex_col0.(*model.Vector).Rows(); i++ {
					So(ex_col0.(*model.Vector).Get(i).Value(), ShouldEqual, tab.(*model.Table).GetColumnByIndex(0).Get(i).Value())
					So(tab.(*model.Table).GetColumnByIndex(1).Get(i).Value(), ShouldEqual, "void(null)")
				}
				_, err = ddb.Upload(map[string]model.DataForm{"tab": tab})
				So(err, ShouldBeNil)
				var assert_s = "res = bool([]);res.append!(eqObj(tab.column(0), 1..100));res.append!(tab.column(1).isNull());all(res)"
				res, _ := ddb.RunScript(assert_s)
				So(res.(*model.Scalar).Value(), ShouldBeTrue)
			})
			Convey("Test_sql_select_NULL_with_no_other_data", func() {
				var sql string = "select NULL as val from table(1..100 as id)"
				tab, err := ddb.RunScript(sql)
				So(err, ShouldBeNil)
				So(tab.(*model.Table).Columns(), ShouldEqual, 1)
				So(tab.(*model.Table).Rows(), ShouldEqual, 1)
				So(tab.(*model.Table).GetColumnByIndex(0).GetDataType(), ShouldEqual, model.DtVoid)
				So(tab.(*model.Table).GetColumnByIndex(0).Get(0).Value(), ShouldEqual, "void(null)")
				_, err = ddb.Upload(map[string]model.DataForm{"tab": tab})
				So(err, ShouldBeNil)
				var assert_s = "res = bool([]);res.append!(tab.column(0).isNull());all(res)"
				res, _ := ddb.RunScript(assert_s)
				So(res.(*model.Scalar).Value(), ShouldBeTrue)
			})
			Convey("Test_sql_select_NULL_from_huge_table", func() {
				// NOTE(slshen) The case also test performance, the whole case should be completed within 30 seconds
				t1, _ := ddb.RunScript("t = table(1..14000000 as id, rand(`a`c`sd``qx, 14000000) as sym);t")
				var sql string = "select *, NULL as null_c1, NULL as null_c2, NULL as null_c3, NULL as null_c4 from t"
				tab, err := ddb.RunScript(sql)
				So(err, ShouldBeNil)
				So(tab.(*model.Table).Columns(), ShouldEqual, 6)
				So(tab.(*model.Table).Rows(), ShouldEqual, 14000000)
				rows := t1.(*model.Table).Rows()
				t1Col0 := t1.(*model.Table).GetColumnByIndex(0)
				t1Col1 := t1.(*model.Table).GetColumnByIndex(1)
				tabCol0 := tab.(*model.Table).GetColumnByIndex(0)
				tabCol1 := tab.(*model.Table).GetColumnByIndex(1)
				for i := 0; i < rows; i++ {
					// fmt.Println("loop 1: ", i)
					assert.Equal(t, t1Col0.Get(i).Value(), tabCol0.Get(i).Value())
					assert.Equal(t, t1Col1.Get(i).Value(), tabCol1.Get(i).Value())
				}
				for i := 2; i < tab.(*model.Table).Columns(); i++ {
					// fmt.Println("loop 2: ", i)
					assert.Equal(t, tab.(*model.Table).GetColumnNames()[i], "null_c"+strconv.Itoa(i-1))
					tabRows := tab.(*model.Table).Rows()
					col := tab.(*model.Table).GetColumnByIndex(i)
					for j := 0; j < tabRows; j++ {
						assert.Equal(t, col.IsNull(j), true)
					}
				}
			})

		})
	})
}
