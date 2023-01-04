package script

import (
	"fmt"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
)

// CheckFunction checks whether the RunFunc is valid.
func CheckFunction(db api.DolphinDB) {
	l, err := model.NewDataTypeListFromRawData(model.DtDouble, []float64{1.5, 2.5, 7})
	util.AssertNil(err)

	df, err := db.RunFunc("sum", []model.DataForm{model.NewVector(l)})
	util.AssertNil(err)
	util.AssertEqual(df.String(), "double(11)")

	_, err = db.RunScript("def f(a,b) {return a+b};")
	util.AssertNil(err)

	arg0, err := model.NewDataType(model.DtInt, int32(1))
	util.AssertNil(err)

	arg1, err := model.NewDataType(model.DtInt, int32(2))
	util.AssertNil(err)

	df, err = db.RunFunc("f", []model.DataForm{model.NewScalar(arg0),
		model.NewScalar(arg1)})
	util.AssertNil(err)
	util.AssertEqual(df.String(), "int(3)")

	fmt.Println("CheckFunction Successful")
}
