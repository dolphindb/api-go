package test

import (
	"context"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

type Sample struct {
	Cbool          []byte            `dolphindb:"column:cbool;type:bool"`
	Cchar          []byte            `dolphindb:"column:cchar;type:char"`
	Cshort         []int16           `dolphindb:"column:cshort;type:short"`
	Cint           []int32           `dolphindb:"column:cint;type:int"`
	Clong          []int64           `dolphindb:"column:clong;type:long"`
	Cfloat         []float32         `dolphindb:"column:cfloat;type:float"`
	Cdouble        []float64         `dolphindb:"column:cdouble;type:double"`
	Cdate          []time.Time       `dolphindb:"column:cdate;type:date"`
	Cdatetime      []time.Time       `dolphindb:"column:cdatetime;type:datetime"`
	Cminute        []time.Time       `dolphindb:"column:cminute;type:minute"`
	Csecond        []time.Time       `dolphindb:"column:csecond;type:second"`
	Cmonth         []time.Time       `dolphindb:"column:cmonth;type:month"`
	Cdatehour      []time.Time       `dolphindb:"column:cdatehour;type:datehour"`
	Cnanotime      []time.Time       `dolphindb:"column:cnanotime;type:nanotime"`
	Cnanotimestamp []time.Time       `dolphindb:"column:cnanotimestamp;type:nanotimestamp"`
	Ctimestamp     []time.Time       `dolphindb:"column:ctimestamp;type:timestamp"`
	Cblob          [][]byte          `dolphindb:"column:cblob;type:blob"`
	Cdecimal32     *model.Decimal32s `dolphindb:"column:cdecimal32;type:decimal32"`
	Cdecimal64     *model.Decimal64s `dolphindb:"column:cdecimal64;type:decimal64"`
	Cstring        []string          `dolphindb:"column:cstring;type:string"`
	Csymbol        []string          `dolphindb:"column:csymbol;type:symbol"`
	Cuuid          []string          `dolphindb:"column:cuuid;type:uuid"`
	Cip            []string          `dolphindb:"column:cip;type:ipaddr"`
	Cint128        []string          `dolphindb:"column:cint128;type:int128"`
}

func TestNewTableFromStruct(t *testing.T) {
	Convey("test_NewTableFromStruct", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("test_NewTableFromStruct_with_all_datatype", func() {
			sam := &Sample{
				Cbool:          []byte{1, 0, model.NullBool},
				Cchar:          []byte{1, 2, model.NullChar},
				Cshort:         []int16{1, 2, model.NullShort},
				Cint:           []int32{1, 2, model.NullInt},
				Clong:          []int64{1, 2, model.NullLong},
				Cfloat:         []float32{1, 2, model.NullFloat},
				Cdouble:        []float64{1, 2, model.NullDouble},
				Cdate:          []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cdatetime:      []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cminute:        []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Csecond:        []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cmonth:         []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cdatehour:      []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cnanotime:      []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cnanotimestamp: []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Ctimestamp:     []time.Time{time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), model.NullTime},
				Cblob:          [][]byte{[]byte("blob1"), []byte("blob2"), model.NullBlob},
				Cdecimal32:     &model.Decimal32s{2, []float64{1.32244, -3.3, model.NullDecimal32Value}},
				Cdecimal64:     &model.Decimal64s{11, []float64{1.32244, -3.3, model.NullDecimal64Value}},
				Cstring:        []string{"智臾科技", "$/-*&(!~;,'.,[]:", ""},
				Csymbol:        []string{"智臾科技", "$/-*&(!~;,'.,[]:", ""},
				Cuuid:          []string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000"},
				Cip:            []string{"35dd:4ae6:b1b1:3da9:d777:d2ab:74cc:e05", "192.168.1.1", "0.0.0.0"},
				Cint128:        []string{"e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000"},
			}
			tb, err := model.NewTableFromStruct(sam)
			So(err, ShouldBeNil)
			_, err = ddb.Upload(map[string]model.DataForm{"tab": tb})
			So(err, ShouldBeNil)
			res, _ := ddb.RunScript(`cbool=bool([1,0,NULL]);
									cchar=char([1,2,NULL]);
									cshort=short([1,2,NULL]);
									cint=int([1,2,NULL]);
									clong=long([1,2,NULL]);
									cfloat=float([1,2,NULL]);
									cdouble=double([1,2,NULL]);
									cdate=date([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cdatetime=datetime([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cminute=minute([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									csecond=second([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cmonth=month([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cdatehour=datehour([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cnanotime=nanotime([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cnanotimestamp=nanotimestamp([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									ctimestamp=timestamp([1969.12.31 00:00:00, 1970.01.01 00:00:00, NULL]);
									cblob=blob(["blob1","blob2",""]);
									cdecimal32=decimal32([1.32244, -3.3,NULL],2);
									cdecimal64=decimal64([1.32244, -3.3,NULL],11);
									cstring=string(["智臾科技", "$/-*&(!~;,'.,[]:", ""]);
									csymbol=symbol(["智臾科技", "$/-*&(!~;,'.,[]:", ""]);
									cuuid=uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000"]);
									cip=ipaddr(["35dd:4ae6:b1b1:3da9:d777:d2ab:74cc:e05", "192.168.1.1", "0.0.0.0"]);
									cint128=int128(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000"]);
									pt=table(cbool,cchar, cshort, cint, clong, cfloat, cdouble, cdate, cdatetime, cminute, csecond, cmonth, cdatehour, cnanotime, cnanotimestamp, ctimestamp, cblob, cdecimal32, cdecimal64, cstring, csymbol, cuuid, cip, cint128);
									eqObj(pt.values(), tab.values())`)
			So(res.(*model.Scalar).Value(), ShouldBeTrue)

		})
	})
	Convey("test_NewTableFromStruct_parameter", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("test_NewTableFromStruct_with_val_nil", func() {
			_ = ddb
			_, err = model.NewTableFromStruct(nil)
			So(err.Error(), ShouldEqual, "Input should not be nil")
		})
		Convey("test_NewTableFromStruct_with_error_colType", func() {
			type Example struct {
				Price []float64 `dolphindb:"column:price;type:float64"`
			}
			act := &Example{
				Price: []float64{1.23125},
			}
			_, err = model.NewTableFromStruct(act)
			So(err.Error(), ShouldEqual, "Invalid type float64")
		})
	})
}
