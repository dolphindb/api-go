package test

import (
	"bytes"
	"testing"

	"github.com/dolphindb/api-go/v3/dialer/protocol"
	"github.com/dolphindb/api-go/v3/model"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Dataform(t *testing.T) {
	Convey("Test_Dataform", t, func() {
		buf := bytes.NewBufferString("")
		rd := protocol.NewReader(buf)
		df, err := model.ParseDataForm(rd, protocol.LittleEndian)
		So(err, ShouldNotBeNil)

		buf.Write([]byte{10, 10})
		df, err = model.ParseDataForm(rd, protocol.LittleEndian)
		So(err, ShouldBeNil)
		So(df, ShouldBeNil)
	})
}
