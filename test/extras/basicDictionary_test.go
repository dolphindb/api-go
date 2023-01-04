package test

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/dialer/protocol"
	"github.com/dolphindb/api-go/model"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Dictionary(t *testing.T) {
	Convey("Test_dictionary", t, func() {
		buf := bytes.NewBuffer(nil)
		wr := protocol.NewWriter(buf)

		key, err := model.NewDataType(model.DtString, "str2")
		So(err, ShouldBeNil)

		v, err := model.NewDataType(model.DtString, "val")
		So(err, ShouldBeNil)

		dtStr, err := model.NewDataTypeListFromRawData(model.DtString, []string{"str1", "str2"})
		So(err, ShouldBeNil)

		val := model.NewVector(dtStr)

		emptyVal := model.NewVector(dtStr)
		emptyVal.Data = nil

		dict := model.NewDictionary(nil, val)
		So(dict, ShouldNotBeNil)

		dict.Values = nil
		err = dict.Render(wr, protocol.LittleEndian)
		So(err, ShouldBeNil)

		dt, err := dict.Get("key")
		So(dt, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(dict.String(), ShouldEqual, "")

		dict.Set(nil, nil)

		dict.Keys = emptyVal
		dt, err = dict.Get("key")
		So(dt, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(dict.String(), ShouldEqual, "")

		dict.Set(nil, nil)

		dict.Keys = val
		dt, err = dict.Get("key")
		So(dt, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(dict.String(), ShouldEqual, "")

		dict.Set(nil, nil)

		dict.Keys = val
		dict.Values = emptyVal
		dt, err = dict.Get("key")
		So(dt, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(dict.String(), ShouldEqual, "")

		dict.Set(nil, nil)

		dict.Values = val.GetSubvector([]int{0})
		dt, err = dict.Get("str2")
		So(dt, ShouldBeNil)
		So(err, ShouldNotBeNil)

		dt, err = dict.Get("str1")
		So(err, ShouldBeNil)
		So(dt.String(), ShouldEqual, "str1")

		dict.Values = val
		dict.Set(key, v)
		dt, err = dict.Get("val")
		So(err, ShouldBeNil)
		So(dt.String(), ShouldEqual, "val")
	})
}
