package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func StringToBytes(data string) []byte {
	return []byte(data)
}

func TestTableDataType(t *testing.T) {
	Convey("Test table prepare", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Test table only one rows:", func() {
			Convey("Test table integer type:", func() {
				s, err := db.RunScript(`
					table([68] as intv,
					long([-94]) as longv,
					short([65]) as shortv,
					char([0]) as charv,
					[true] as boolv
					)`)
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				Convey("Test table int type:", func() {
					reint := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reint.GetDataType(), ShouldEqual, model.DtInt)
					So(reint.GetDataForm(), ShouldResemble, model.DfVector)
					So(reint.Rows(), ShouldEqual, 1)
					re := reint.Data.Value()
					tmp := []int{68}
					for i := 0; i < reint.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
				Convey("Test table long type:", func() {
					relong := memTable.GetColumnByName(memTable.GetColumnNames()[1])
					So(relong.GetDataType(), ShouldEqual, model.DtLong)
					So(relong.GetDataForm(), ShouldResemble, model.DfVector)
					So(relong.Rows(), ShouldEqual, 1)
					re := relong.Data.Value()
					tmp := []int64{-94}
					for i := 0; i < relong.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
				Convey("Test table short type:", func() {
					reshort := memTable.GetColumnByName(memTable.GetColumnNames()[2])
					So(reshort.GetDataType(), ShouldEqual, model.DtShort)
					So(reshort.GetDataForm(), ShouldResemble, model.DfVector)
					So(reshort.Rows(), ShouldEqual, 1)
					re := reshort.Data.Value()
					tmp := []int16{65}
					for i := 0; i < reshort.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
				Convey("Test table char type:", func() {
					rechar := memTable.GetColumnByName(memTable.GetColumnNames()[3])
					So(rechar.GetDataType(), ShouldEqual, model.DtChar)
					So(rechar.GetDataForm(), ShouldResemble, model.DfVector)
					So(rechar.Rows(), ShouldEqual, 1)
					re := rechar.Data.Value()
					tmp := []byte{0}
					for i := 0; i < rechar.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
				Convey("Test table bool type:", func() {
					rebool := memTable.GetColumnByName(memTable.GetColumnNames()[4])
					So(rebool.GetDataType(), ShouldEqual, model.DtBool)
					So(rebool.GetDataForm(), ShouldResemble, model.DfVector)
					So(rebool.Rows(), ShouldEqual, 1)
					re := rebool.Data.Value()
					tmp := []bool{true}
					for i := 0; i < rebool.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
			})
			Convey("Test table string and symbol type:", func() {
				s, err := db.RunScript(`
			table(symbol(["AAPL"]) as sym,
				"A" + string(1) as stringv)`)
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				Convey("Test table symbol type:", func() {
					resym := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(resym.GetDataType(), ShouldEqual, model.DtSymbol)
					So(resym.GetDataForm(), ShouldResemble, model.DfVector)
					So(resym.Rows(), ShouldEqual, 1)
					re := resym.Data.Value()
					tmp := []string{"AAPL"}
					for i := 0; i < resym.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
				Convey("Test table string type:", func() {
					reString := memTable.GetColumnByName(memTable.GetColumnNames()[1])
					So(reString.GetDataType(), ShouldEqual, model.DtString)
					So(reString.GetDataForm(), ShouldResemble, model.DfVector)
					So(reString.Rows(), ShouldEqual, 1)
					re := reString.Data.Value()
					tmp := []string{"A1"}
					for i := 0; i < reString.Rows(); i++ {
						So(re[i], ShouldEqual, tmp[i])
					}
				})
			})
			Convey("Test table temporal type:", func() {
				s, err := db.RunScript(`
			table([1970.01.06] as datev,
				[1970.01.01T00:01:34] as datetimev,
				[1969.12.31T23:59:59.946] as timestampv,
				[1968.01M] as month,
				[00:00:00.007] as timev,
				[00:01:02] as secondv,
				[00:35m] as minutev,
				[00:00:00.000000032] as nanotimev,
				[1969.12.31T23:59:59.999999942] as nanotimestampv)`)
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				Convey("Test table date type:", func() {
					redate := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(redate.GetDataType(), ShouldEqual, model.DtDate)
					So(redate.GetDataForm(), ShouldResemble, model.DfVector)
					So(redate.Rows(), ShouldEqual, 1)
					re := redate.Data.Value()
					datev := time.Date(1970, time.January, 06, 0, 0, 0, 0, time.UTC)
					tmp := []time.Time{datev}
					for i := 0; i < redate.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test table datetime type:", func() {
					redatetime := memTable.GetColumnByName(memTable.GetColumnNames()[1])
					So(redatetime.GetDataType(), ShouldEqual, model.DtDatetime)
					So(redatetime.GetDataForm(), ShouldResemble, model.DfVector)
					So(redatetime.Rows(), ShouldEqual, 1)
					re := redatetime.Data.Value()
					datetimev := time.Date(1970, time.January, 01, 0, 01, 34, 0, time.UTC)
					tmp := []time.Time{datetimev}
					for i := 0; i < redatetime.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
			})
		})
		Convey("Test table insert into one rows", func() {
			Convey("Test table insert into int and long and short and char and bool rows", func() {
				_, err := db.RunScript(`t=table(100:0, ["id", "longv", "shortv", "charv", "boolv"], 
			[INT, LONG, SHORT, CHAR, BOOL])`)
				So(err, ShouldBeNil)
				var id int32 = 10
				var longv int64 = 11
				var shortv int16 = 9
				var charv byte = 23
				var boolv bool = true
				_, err = db.RunScript(fmt.Sprintf("insert into t values(%v, %v, %v, %v, %v)", id, longv, shortv, charv, boolv))
				So(err, ShouldBeNil)
				s, err := db.RunScript("t")
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				for _, i := range memTable.GetColumnNames() {
					col := memTable.GetColumnByName(i)
					So(col.GetDataForm(), ShouldResemble, model.DfVector)
					So(col.Rows(), ShouldEqual, 1)
				}
				reint := memTable.GetColumnByName(memTable.GetColumnNames()[0])
				So(reint.GetDataType(), ShouldEqual, model.DtInt)
				assert.Equal(t, reint.String(), "vector<int>([10])")
				relong := memTable.GetColumnByName(memTable.GetColumnNames()[1])
				So(relong.GetDataType(), ShouldEqual, model.DtLong)
				assert.Equal(t, relong.String(), "vector<long>([11])")
				reshort := memTable.GetColumnByName(memTable.GetColumnNames()[2])
				So(reshort.GetDataType(), ShouldEqual, model.DtShort)
				assert.Equal(t, reshort.String(), "vector<short>([9])")
				rechar := memTable.GetColumnByName(memTable.GetColumnNames()[3])
				So(rechar.GetDataType(), ShouldEqual, model.DtChar)
				assert.Equal(t, rechar.String(), "vector<char>([23])")
				rebool := memTable.GetColumnByName(memTable.GetColumnNames()[4])
				So(rebool.GetDataType(), ShouldEqual, model.DtBool)
				assert.Equal(t, rebool.String(), "vector<bool>([true])")
			})
			Convey("Test table insert into doublev and floatv rows", func() {
				_, err := db.RunScript(`t=table(100:0, ["doublev", "floatv"], [DOUBLE, FLOAT])`)
				So(err, ShouldBeNil)
				var doublev float64 = 22.8
				var floatv float32 = 10.5
				_, err = db.RunScript(fmt.Sprintf("insert into t values(%v, %v)", doublev, floatv))
				So(err, ShouldBeNil)
				s, err := db.RunScript("t")
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				for _, i := range memTable.GetColumnNames() {
					col := memTable.GetColumnByName(i)
					So(col.GetDataForm(), ShouldResemble, model.DfVector)
					So(col.Rows(), ShouldEqual, 1)
				}
				redouble := memTable.GetColumnByName(memTable.GetColumnNames()[0])
				So(redouble.GetDataType(), ShouldEqual, model.DtDouble)
				assert.Equal(t, redouble.String(), "vector<double>([22.8])")
				refloat := memTable.GetColumnByName(memTable.GetColumnNames()[1])
				So(refloat.GetDataType(), ShouldEqual, model.DtFloat)
				assert.Equal(t, refloat.String(), "vector<float>([10.5])")
			})
			Convey("Test table insert into symbol and string rows", func() {
				_, err := db.RunScript(`t=table(100:0, ["sym", "stringv"], [SYMBOL, STRING])`)
				So(err, ShouldBeNil)
				var symv string = "AAPL"
				var colv string = "A1"
				_, err = db.RunScript(fmt.Sprintf("insert into t values('%s', '%s')", symv, colv))
				So(err, ShouldBeNil)
				s, err := db.RunScript("t")
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				for _, i := range memTable.GetColumnNames() {
					col := memTable.GetColumnByName(i)
					So(col.GetDataForm(), ShouldResemble, model.DfVector)
					So(col.Rows(), ShouldEqual, 1)
				}
				resymbol := memTable.GetColumnByName(memTable.GetColumnNames()[0])
				So(resymbol.GetDataType(), ShouldEqual, model.DtSymbol)
				So(resymbol.String(), ShouldEqual, "vector<symbol>([AAPL])")
				reString := memTable.GetColumnByName(memTable.GetColumnNames()[1])
				So(reString.GetDataType(), ShouldEqual, model.DtString)
				So(reString.String(), ShouldEqual, "vector<string>([A1])")
			})
			Convey("Test table insert into temporal rows", func() {
				_, err := db.RunScript(`t=table(100:0, ["datev", "datetimev", "timestampv", "nanotimestampv", "datehourv", "monthv", "timev", "secondv", "minutev", "nanotimev"], 
			[DATE, DATETIME, TIMESTAMP, NANOTIMESTAMP, DATEHOUR, MONTH, TIME, SECOND, MINUTE, NANOTIME])`)
				So(err, ShouldBeNil)
				datev := time.Date(1969, time.December, 31, 0, 0, 0, 0, time.UTC)
				datec := datev.Format("2006.01.02T15:04:05.000")
				datetimev := time.Date(1969, time.December, 31, 23, 56, 59, 0, time.UTC)
				datetimec := datetimev.Format("2006.01.02T15:04:05.000")
				timestampv := time.Date(1969, time.December, 31, 23, 56, 59, 123*1000000, time.UTC)
				timestampc := timestampv.Format("2006.01.02T15:04:05.000")
				nanotimestampv := time.Date(1969, time.December, 31, 23, 56, 59, 123000999, time.UTC)
				nanotimestampc := nanotimestampv.Format("2006.01.02T15:04:05.000000000")
				datehourv := time.Date(1969, time.December, 31, 23, 00, 00, 0, time.UTC)
				datehourc := datehourv.Format("2006.01.02T15:00:00.000")
				monthv := time.Date(1969, time.December, 31, 0, 0, 0, 0, time.UTC)
				monthc := monthv.Format("2006.01.01T00:00:00.000")
				timev := time.Date(1970, time.January, 1, 23, 56, 59, 123*1000000, time.UTC)
				timec := timev.Format("2006.01.02T15:04:05.000")
				secondv := time.Date(1970, time.January, 1, 23, 56, 59, 0, time.UTC)
				secondc := secondv.Format("15:04:05.000")
				minutev := time.Date(1970, time.January, 1, 23, 56, 0, 0, time.UTC)
				minutec := minutev.Format("15:04m")
				nanotimev := time.Date(1970, time.January, 1, 23, 56, 59, 123123456, time.UTC)
				nanotimec := nanotimev.Format("2006.01.02T15:04:05.000000000")
				_, err = db.RunScript(fmt.Sprintf("insert into t values(%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)",
					datec, datetimec, timestampc, nanotimestampc, datehourc, monthc, timec, secondc, minutec, nanotimec))
				So(err, ShouldBeNil)
				s, err := db.RunScript("t")
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				for _, i := range memTable.GetColumnNames() {
					col := memTable.GetColumnByName(i)
					So(col.GetDataForm(), ShouldResemble, model.DfVector)
					So(col.Rows(), ShouldEqual, 1)
				}
				redate := memTable.GetColumnByName(memTable.GetColumnNames()[0])
				So(redate.GetDataType(), ShouldEqual, model.DtDate)
				re := redate.Data.Value()
				tmp := []time.Time{datev}
				assert.Equal(t, re[0], tmp[0])
				redatetime := memTable.GetColumnByName(memTable.GetColumnNames()[1])
				So(redatetime.GetDataType(), ShouldEqual, model.DtDatetime)
				re = redatetime.Data.Value()
				tmp = []time.Time{datetimev}
				assert.Equal(t, re[0], tmp[0])
				retimestamp := memTable.GetColumnByName(memTable.GetColumnNames()[2])
				So(retimestamp.GetDataType(), ShouldEqual, model.DtTimestamp)
				re = retimestamp.Data.Value()
				tmp = []time.Time{timestampv}
				assert.Equal(t, re[0], tmp[0])
				renanotimestamp := memTable.GetColumnByName(memTable.GetColumnNames()[3])
				So(renanotimestamp.GetDataType(), ShouldEqual, model.DtNanoTimestamp)
				re = renanotimestamp.Data.Value()
				tmp = []time.Time{nanotimestampv}
				assert.Equal(t, re[0], tmp[0])
				redatehour := memTable.GetColumnByName(memTable.GetColumnNames()[4])
				So(redatehour.GetDataType(), ShouldEqual, model.DtDateHour)
				re = redatehour.Data.Value()
				tmp = []time.Time{datehourv}
				assert.Equal(t, re[0], tmp[0])
				remonth := memTable.GetColumnByName(memTable.GetColumnNames()[5])
				So(remonth.GetDataType(), ShouldEqual, model.DtMonth)
				re = redate.Data.Value()
				tmp = []time.Time{monthv}
				assert.Equal(t, re[0], tmp[0])
				retime := memTable.GetColumnByName(memTable.GetColumnNames()[6])
				So(retime.GetDataType(), ShouldEqual, model.DtTime)
				re = retime.Data.Value()
				tmp = []time.Time{timev}
				assert.Equal(t, re[0], tmp[0])
				resecond := memTable.GetColumnByName(memTable.GetColumnNames()[7])
				So(resecond.GetDataType(), ShouldEqual, model.DtSecond)
				re = resecond.Data.Value()
				tmp = []time.Time{secondv}
				assert.Equal(t, re[0], tmp[0])
				reminute := memTable.GetColumnByName(memTable.GetColumnNames()[8])
				So(reminute.GetDataType(), ShouldEqual, model.DtMinute)
				re = reminute.Data.Value()
				tmp = []time.Time{minutev}
				assert.Equal(t, re[0], tmp[0])
				renanotime := memTable.GetColumnByName(memTable.GetColumnNames()[9])
				So(renanotime.GetDataType(), ShouldEqual, model.DtNanoTime)
				re = renanotime.Data.Value()
				tmp = []time.Time{nanotimev}
				assert.Equal(t, re[0], tmp[0])
			})
			Convey("Test table insert into special type rows", func() {
				_, err := db.RunScript(`t=table(100:0, ["uuidv", "int128v", "blobv", "ipv"], [UUID, INT128, BLOB, IPADDR])`)
				So(err, ShouldBeNil)
				uuidv := `uuid("7d943e7f-5660-e015-a895-fa4da2b36c43")`
				int128v := `int128("7667974ea2fb155252559cc28b4a8efa")`
				ipaddrv := `ipaddr("a9b7:f65:9be1:20fd:741a:97ac:6ce5:1dd")`
				blobv := `blob("ALMS")`
				_, err = db.RunScript(fmt.Sprintf("insert into t values(%s, %s, %s, %s)", uuidv, int128v, blobv, ipaddrv))
				So(err, ShouldBeNil)
				s, err := db.RunScript("t")
				So(err, ShouldBeNil)
				memTable := s.(*model.Table)
				for _, i := range memTable.GetColumnNames() {
					col := memTable.GetColumnByName(i)
					So(col.GetDataForm(), ShouldResemble, model.DfVector)
					So(col.Rows(), ShouldEqual, 1)
				}
				reuuid := memTable.GetColumnByName(memTable.GetColumnNames()[0])
				So(reuuid.GetDataType(), ShouldEqual, model.DtUUID)
				So(reuuid.String(), ShouldEqual, "vector<uuid>([7d943e7f-5660-e015-a895-fa4da2b36c43])")
				reint128 := memTable.GetColumnByName(memTable.GetColumnNames()[1])
				So(reint128.GetDataType(), ShouldEqual, model.DtInt128)
				So(reint128.String(), ShouldEqual, "vector<int128>([7667974ea2fb155252559cc28b4a8efa])")
				reblob := memTable.GetColumnByName(memTable.GetColumnNames()[2])
				So(reblob.GetDataType(), ShouldEqual, model.DtBlob)
				re := reblob.Data.Value()
				tmp := StringToBytes("ALMS")
				So(re[0], ShouldResemble, tmp)
			})
		})
	})
}

func TestTableWithCapacity(t *testing.T) {
	Convey("Test_function_TableWithCapacity_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Drop all Databases", func() {
			dbPaths := []string{DfsDBPath, DiskDBPath}
			for _, dbPath := range dbPaths {
				script := `
				if(existsDatabase("` + dbPath + `")){
						dropDatabase("` + dbPath + `")
				}
				if(exists("` + dbPath + `")){
					rmdir("` + dbPath + `", true)
				}
				`
				_, err = ddb.RunScript(script)
				So(err, ShouldBeNil)
				re, err := ddb.RunScript(`existsDatabase("` + dbPath + `")`)
				So(err, ShouldBeNil)
				isExitsDatabase := re.(*model.Scalar).DataType.Value()
				So(isExitsDatabase, ShouldBeFalse)
			}
		})
		Convey("Test_function_TableWithCapacityRequest_SetSize_10", func() {
			l := new(api.TableWithCapacityRequest).
				SetTableName(MemTableName).SetCapacity(100).SetSize(10).
				SetColNames([]string{"id", "datev", "str"}).
				SetColTypes([]string{"INT", "DATE", "STRING"})
			t, err := ddb.TableWithCapacity(l)
			So(err, ShouldBeNil)
			originID := t.Data.GetColumnByName("id")
			originDatev := t.Data.GetColumnByName("datev")
			So(originID.String(), ShouldEqual, "vector<int>([0, 0, 0, 0, 0, 0, 0, 0, 0, 0])")
			So(originDatev.String(), ShouldEqual, "vector<date>([1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01])")
			_, err = ddb.RunScript(`t=table(1..10 as id, 1969.12.26+ 1..10 as datev, "A"+string(1..10) as str); ` + MemTableName + `.append!(t)`)
			So(err, ShouldBeNil)
			reTmp, err := ddb.RunScript(`select * from ` + MemTableName + ``)
			So(err, ShouldBeNil)
			reTable := reTmp.(*model.Table)
			reID := reTable.GetColumnByName("id")
			reDatev := reTable.GetColumnByName("datev")
			reStr := reTable.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(reDatev.String(), ShouldEqual, "vector<date>([1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1970.01.01, 1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(reStr.String(), ShouldEqual, "vector<string>([, , , , , , , , , , A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})

		Convey("Test_function_TableWithCapacityRequest_SetSize_0", func() {
			l := new(api.TableWithCapacityRequest).
				SetTableName(MemTableName).SetCapacity(100).SetSize(0).
				SetColNames([]string{"id", "datev", "str"}).
				SetColTypes([]string{"INT", "DATE", "STRING"})
			t, err := ddb.TableWithCapacity(l)
			So(err, ShouldBeNil)
			originID := t.Data.GetColumnByName("id")
			originDatev := t.Data.GetColumnByName("datev")
			originstr := t.Data.GetColumnByName("str")
			So(originID.String(), ShouldEqual, "vector<int>([])")
			So(originDatev.String(), ShouldEqual, "vector<date>([])")
			So(originstr.String(), ShouldEqual, "vector<string>([])")
			_, err = ddb.RunScript(`t=table(1..10 as id, 1969.12.26+ 1..10 as datev, "A"+string(1..10) as str); ` + MemTableName + `.append!(t)`)
			So(err, ShouldBeNil)
			reTmp, err := ddb.RunScript(`select * from ` + MemTableName + ``)
			So(err, ShouldBeNil)
			reTable := reTmp.(*model.Table)
			reID := reTable.GetColumnByName("id")
			reDatev := reTable.GetColumnByName("datev")
			reStr := reTable.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(reDatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(reStr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_TableWithCapacityRequest_SetCapacity_1023", func() {
			l := new(api.TableWithCapacityRequest).
				SetTableName(MemTableName).SetCapacity(1023).SetSize(0).
				SetColNames([]string{"id", "datev", "str"}).
				SetColTypes([]string{"INT", "DATE", "STRING"})
			t, err := ddb.TableWithCapacity(l)
			So(err, ShouldBeNil)
			originID := t.Data.GetColumnByName("id")
			originDatev := t.Data.GetColumnByName("datev")
			originstr := t.Data.GetColumnByName("str")
			So(originID.String(), ShouldEqual, "vector<int>([])")
			So(originDatev.String(), ShouldEqual, "vector<date>([])")
			So(originstr.String(), ShouldEqual, "vector<string>([])")
			_, err = ddb.RunScript(`t=table(1..10 as id, 1969.12.26+ 1..10 as datev, "A"+string(1..10) as str); ` + MemTableName + `.append!(t)`)
			So(err, ShouldBeNil)
			reTmp, err := ddb.RunScript(`select * from ` + MemTableName + ``)
			So(err, ShouldBeNil)
			reTable := reTmp.(*model.Table)
			reID := reTable.GetColumnByName("id")
			reDatev := reTable.GetColumnByName("datev")
			reStr := reTable.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(reDatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(reStr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
		Convey("Test_function_TableWithCapacityRequest_SetCapacity_1025", func() {
			l := new(api.TableWithCapacityRequest).
				SetTableName(MemTableName).SetCapacity(1025).SetSize(0).
				SetColNames([]string{"id", "datev", "str"}).
				SetColTypes([]string{"INT", "DATE", "STRING"})
			t, err := ddb.TableWithCapacity(l)
			So(err, ShouldBeNil)
			originID := t.Data.GetColumnByName("id")
			originDatev := t.Data.GetColumnByName("datev")
			originstr := t.Data.GetColumnByName("str")
			So(originID.String(), ShouldEqual, "vector<int>([])")
			So(originDatev.String(), ShouldEqual, "vector<date>([])")
			So(originstr.String(), ShouldEqual, "vector<string>([])")
			_, err = ddb.RunScript(`t=table(1..10 as id, 1969.12.26+ 1..10 as datev, "A"+string(1..10) as str); ` + MemTableName + `.append!(t)`)
			So(err, ShouldBeNil)
			reTmp, err := ddb.RunScript(`select * from ` + MemTableName + ``)
			So(err, ShouldBeNil)
			reTable := reTmp.(*model.Table)
			reID := reTable.GetColumnByName("id")
			reDatev := reTable.GetColumnByName("datev")
			reStr := reTable.GetColumnByName("str")
			So(reID.String(), ShouldEqual, "vector<int>([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])")
			So(reDatev.String(), ShouldEqual, "vector<date>([1969.12.27, 1969.12.28, 1969.12.29, 1969.12.30, 1969.12.31, 1970.01.01, 1970.01.02, 1970.01.03, 1970.01.04, 1970.01.05])")
			So(reStr.String(), ShouldEqual, "vector<string>([A1, A2, A3, A4, A5, A6, A7, A8, A9, A10])")
		})
	})
}

func TestTable(t *testing.T) {
	Convey("Test_function_Table_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), setup.Address, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("Drop all Databases", func() {
			dbPaths := []string{DfsDBPath, DiskDBPath}
			for _, dbPath := range dbPaths {
				script := `
				if(existsDatabase("` + dbPath + `")){
						dropDatabase("` + dbPath + `")
				}
				if(exists("` + dbPath + `")){
					rmdir("` + dbPath + `", true)
				}
				`
				_, err = ddb.RunScript(script)
				So(err, ShouldBeNil)
				re, err := ddb.RunScript(`existsDatabase("` + dbPath + `")`)
				So(err, ShouldBeNil)
				isExitsDatabase := re.(*model.Scalar).DataType.Value()
				So(isExitsDatabase, ShouldBeFalse)
			}
		})
		Convey("Test_function_Table", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "`XOM`GS`AAPL").
				AddTableParam("x", "102.1 33.4 73.6")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
		})
		Convey("Test_function_Table_1023", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "take(`XOM`GS`AAPL, 1023)").
				AddTableParam("x", "take(102.1 33.4 73.6, 1023)")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
		})
		Convey("Test_function_Table_1025", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "take(`XOM`GS`AAPL, 1025)").
				AddTableParam("x", "take(102.1 33.4 73.6, 1025)")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
		})
		Convey("Test_function_Table_3000000", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "take(`XOM`GS`AAPL, 3000000)").
				AddTableParam("x", "take(102.1 33.4 73.6, 3000000)")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
		})
		Convey("Test_function_Table_GetHandle", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "`XOM`GS`AAPL").
				AddTableParam("x", "102.1 33.4 73.6")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
			rehandle := origintable.GetHandle()
			So(rehandle, ShouldEqual, MemTableName)
		})
		Convey("Test_function_Table_GetSession", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "`XOM`GS`AAPL").
				AddTableParam("x", "102.1 33.4 73.6")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
			reSession := origintable.GetSession()
			So(reSession, ShouldNotBeNil)
		})
		Convey("Test_function_Table_String", func() {
			newTable := new(api.TableRequest).
				SetTableName(MemTableName).
				AddTableParam("id", "`XOM`GS`AAPL").
				AddTableParam("x", "102.1 33.4 73.6")
			origintable, err := ddb.Table(newTable)
			So(err, ShouldBeNil)
			reTable, err := ddb.RunScript("select * from " + MemTableName + "")
			reTablex := reTable.(*model.Table)
			So(err, ShouldBeNil)
			res := CompareTablesDataformTable(reTablex, origintable)
			So(res, ShouldBeTrue)
			retostring := origintable.String()
			So(retostring, ShouldEqual, reTable.String())
		})
		Convey("Test_function_Table_GetRowJson_index_gt_rows", func() {

			temp, err := ddb.RunScript("table([1] as col1,[`a] as col2, [3.213] as col3)")
			if err != nil {
				panic(err)
			}
			tb := temp.(*model.Table)
			So(tb.GetRowJSON(2), ShouldEqual, "")

		})
		Convey("Test_function_Table_GetRowJson_normal", func() {
			rand.Seed(time.Now().UnixNano())
			// min := int32(-10000)
			// max := int32(10000)

			// min_f := float64(-10.5)
			// max_f := float64(100.05)
			col0, _ := model.NewDataTypeListFromRawData(model.DtString,
				[]string{
					"1",
					"2",
					"",
				})
			col1, _ := model.NewDataTypeListFromRawData(model.DtInt,
				[]int32{
					// rand.Int31n(max-min-1) + min + 1,
					// rand.Int31n(max-min-1) + min + 1,
					// rand.Int31n(max-min-1) + min + 1,
					0,
					-1,
					model.NullInt,
				})
			col2, _ := model.NewDataTypeListFromRawData(model.DtDouble,
				[]float64{
					// rand.Float64()*(max_f-min_f) + min_f,
					// rand.Float64()*(max_f-min_f) + min_f,
					// rand.Float64()*(max_f-min_f) + min_f,
					model.NullDouble,
					2.331245,
					-235.1235666,
				})
			col3, _ := model.NewDataTypeListFromRawData(model.DtBool,
				[]byte{
					// rand.Float64()*(max_f-min_f) + min_f,
					// rand.Float64()*(max_f-min_f) + min_f,
					// rand.Float64()*(max_f-min_f) + min_f,
					model.NullBool,
					1,
					0,
				})
			col4, _ := model.NewDataTypeListFromRawData(model.DtDate,
				[]time.Time{
					// rand.Float64()*(max_f-min_f) + min_f,
					// rand.Float64()*(max_f-min_f) + min_f,
					// rand.Float64()*(max_f-min_f) + min_f,
					model.NullTime,
					time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
					time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
				})
			col5, _ := model.NewDataTypeListFromRawData(model.DtDecimal32,
				&model.Decimal32s{Scale: 2,
					Value: []float64{
						0.0001,
						-23.3554662,
						float64(model.NullDecimal32Value)},
				})

			col6, _ := model.NewDataTypeListFromRawData(model.DtDecimal64,
				&model.Decimal64s{Scale: 11,
					Value: []float64{
						0.01,
						-23.3554662,
						float64(model.NullDecimal64Value)},
				})

			tb := model.NewTable([]string{"sym", "int", "double", "bool", "date", "deci32", "deci64"},
				[]*model.Vector{model.NewVector(col0),
					model.NewVector(col1),
					model.NewVector(col2),
					model.NewVector(col3),
					model.NewVector(col4),
					model.NewVector(col5),
					model.NewVector(col6),
				})

			for i := 0; i < tb.Rows(); i++ {
				fmt.Println(tb.GetRowJSON(i))
			}
			ex0 := `{"sym":"1","int":"0","double":"","bool":"","date":"","deci32":"0.00","deci64":"0.01000000000"}`
			ex1 := `{"sym":"2","int":"-1","double":"2.331245","bool":"true","date":"1969.12.31","deci32":"-23.35","deci64":"-23.35546620000"}`
			ex2 := `{"sym":"","int":"","double":"-235.1235666","bool":"false","date":"1970.01.01","deci32":"","deci64":""}`

			So(ex0, ShouldEqual, tb.GetRowJSON(0))
			So(ex1, ShouldEqual, tb.GetRowJSON(1))
			So(ex2, ShouldEqual, tb.GetRowJSON(2))
		})
	})
}
