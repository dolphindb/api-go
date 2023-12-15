package test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

func checkVectorisNull(arr *model.Vector) bool {
	for i := 0; i < arr.Rows(); i++ {
		re := arr.Data.IsNull(i)
		if re != true {
			return false
		}
	}
	return true
}

func TestRunScript(t *testing.T) {
	t.Parallel()
	Convey("test_RunScript_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("test_RunScript_func", func() {
			Convey("test_RunScript_bool_scalar", func() {
				tmp, err := ddb.RunScript("true")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "bool")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				So(re, ShouldEqual, true)
				tmp, err = ddb.RunScript("bool()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_char_scalar", func() {
				tmp, err := ddb.RunScript("'a'")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "char")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				var ex byte = 97
				So(re, ShouldEqual, ex)
				tmp, err = ddb.RunScript("char()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_short_scalar", func() {
				tmp, err := ddb.RunScript("22h")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "short")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				var ex int16 = 22
				So(re, ShouldEqual, ex)
				tmp, err = ddb.RunScript("short()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_int_scalar", func() {
				tmp, err := ddb.RunScript("22")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "int")
				result := tmp.(*model.Scalar)
				re := result.DataType.Value()
				var ex int32 = 22
				So(re, ShouldEqual, ex)
				tmp, err = ddb.RunScript("int()")
				So(err, ShouldBeNil)
				result = tmp.(*model.Scalar)
				re = result.IsNull()
				So(re, ShouldEqual, true)
			})
			Convey("test_RunScript_long_vector", func() {
				tmp, err := ddb.RunScript("22l 200l")
				So(err, ShouldBeNil)
				reType := tmp.GetDataTypeString()
				So(reType, ShouldEqual, "long")
				result := tmp.(*model.Vector)
				re := result.Data.Value()
				var ex1 int64 = 22
				var ex2 int64 = 200
				So(re[0], ShouldEqual, ex1)
				So(re[1], ShouldEqual, ex2)
				tmp, err = ddb.RunScript("take(00i, 10)")
				So(err, ShouldBeNil)
				result = tmp.(*model.Vector)
				rs := checkVectorisNull(result)
				So(rs, ShouldEqual, true)
			})
		})
	})
}

func TestPrint(t *testing.T) {
	t.Parallel()
	Convey("test_print_msg_on_console", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		_, err = ddb.RunScript(`a=int(1);
								b=bool(1);
								c=char(1);
								d=NULL;
								ee=short(1);
								f=long(1);
								g=date(1);
								h=month(1);
								i=time(1);
								j=minute(1);
								k=second(1);
								l=datetime(1);
								m=timestamp(1);
								n=nanotime(1);
								o=nanotimestamp(1);
								p=float(1);
								q=double(1);
								r="1";
								s=uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87");
								ttt=blob(string[1]);
								u=table(1 2 3 as col1, ["a", "b", "c"] as col2);
								v=arrayVector(1 2 3 , 9 9 9)`)
		So(err, ShouldBeNil)

		fw, _ := os.Create("./tmp")
		old := os.Stdout
		os.Stdout = fw
		ddb.RunScript("print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u,v)")
		fw.Close()

		os.Stdout = old
		fr, err := os.OpenFile("./tmp", os.O_RDONLY, 0644)
		So(err, ShouldBeNil)
		if err != nil {
			panic(err)
		}

		reader := bufio.NewReader(fr)
		ex := []string{"1\n", "1\n", "1\n", "1\n", "1\n", "1970.01.02\n", "0000.02M\n", "00:00:00.001\n", "00:01m\n", "00:00:01\n", "1970.01.01T00:00:01\n",
			"1970.01.01T00:00:00.001\n", "00:00:00.000000001\n", "1970.01.01T00:00:00.000000001\n", "1\n", "1\n", "1\n", "5d212a78-cc48-e3b1-4235-b4d91473ee87\n",
			"[\"1\"]\n", "col1 col2\n", "---- ----\n", "1    a   \n", "2    b   \n", "3    c   \n", "\n", "[[9],[9],[9]]\n", ""}
		ind := 0
		for {
			ex_line := ex[ind]
			line, err := reader.ReadString('\n')
			if err != nil && err.Error() != "EOF" {
				fmt.Println("读取文件错误:", err)
				return
			}
			// fmt.Println(line)
			So(line, ShouldEqual, ex_line)
			if err == io.EOF {
				break
			}
			ind++
		}

		So(ddb.Close(), ShouldBeNil)
		So(fr.Close(), ShouldBeNil)
		So(os.Remove("./tmp"), ShouldBeNil)
	})

}
