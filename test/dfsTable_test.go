package test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/model"
	"github.com/dolphindb/api-go/v3/test/setup"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

var host4 = getRandomClusterAddress()

func CreateScript(Num int) (string, string) {
	dbPath := "dfs://" + generateRandomString(5)
	script := `
	dbName="` + dbPath + `";
	if(existsDatabase(dbName)){
		dropDatabase(dbName)
	}
	n=` + strconv.Itoa(Num) + `
	t=table(100:0, ["sym", "boolv", "intv", "longv", "shortv", "doublev", "floatv", "str", "charv", "timestampv", "datev", "datetimev", "monthv", "timev", "minutev", "secondv", "nanotimev", "nanotimestamp", "datehourv", "uuidv", "ipaddrv", "int128v"],
	[SYMBOL, BOOL, INT, LONG, SHORT, DOUBLE, FLOAT, STRING, CHAR, TIMESTAMP, DATE, DATETIME, MONTH, TIME, MINUTE, SECOND, NANOTIME, NANOTIMESTAMP, DATEHOUR, UUID, IPADDR, INT128])
	db=database(dbName, VALUE, ["A", "B", "C", "D", "E", "F"])
	pt=db.createPartitionedTable(t, "pt", "sym")
	sym = take(["A", "B", "C", "D", "E", "F"], n)
	boolv = take([true, false, true, false, false, true, true], n)
	intv = take([91,NULL,69,16,35,NULL,57,-28,-81,26], n)
	longv = take([99,23,92,NULL,49,67,NULL,81,-38,14], n)
	shortv = take([47,26,-39,NULL,97,NULL,4,39,-51,25], n)
	doublev = take([4.7,2.6,-3.9,NULL,9.7,4.9,NULL,3.9,5.1,2.5], n)
	floatv = take([5.2f, 11.3f, -3.9, 1.2f, 7.8f, -4.9f, NULL, 3.9f, 5.1f, 2.5f], n)
	str = take("str" + string(1..10), n)
	charv = take(char([70, 72, 15, 98, 94]), n)
	timestampv = take([2012.01.01T12:23:56.166, NULL, 1970.01.01T12:23:56.148, 1969.12.31T23:59:59.138, 2012.01.01T12:23:56.132], n)
	datev = take([NULL, 1969.01.11, 1970.01.24, 1969.12.31, 2012.03.30], n)
	datetimev = take([NULL, 2012.01.01T12:24:04, 2012.01.01T12:25:04, 2012.01.01T12:24:55, 2012.01.01T12:24:27], n)
	monthv = take([1970.06M, 2014.05M, 1970.06M, 2017.12M, 1969.11M], n)
	timev = take([12:23:56.156, NULL, 12:23:56.206, 12:23:56.132, 12:23:56.201], n)
	minutev = take([12:47m,13:13m, NULL, 13:49m, 13:17m], n)
	secondv = take([NULL, 00:03:11, 00:01:52, 00:02:43, 00:02:08], n)
	nanotimev = take(nanotime(1..10) join nanotime(), n)
	nanotimestampv = take(nanotimestamp(-5..5) join nanotimestamp(), n)
	datehourv = take(datehour([1969.12.01, 1969.01.11, NULL, 1969.12.31, 2012.03.30]), n)
	uuidv = take([uuid("7d943e7f-5660-e015-a895-fa4da2b36c43"), uuid("3272fc73-5a91-34f5-db39-6ee71aa479a4"), uuid("62746671-9870-5b92-6deb-a6f5d59e715e"), uuid("dd05902d-5561-ee7f-6318-41a107371a8d"), uuid("14f82b2a-cf0f-7a0c-4cba-3df7be0ba0fc"), uuid("1f9093c3-9132-7200-4893-0f937a0d52c9")], n)
	ipaddrv = take([ipaddr("a9b7:f65:9be1:20fd:741a:97ac:6ce5:1dd"), ipaddr("8494:3a0e:13db:a097:d3fd:8dc:56e4:faed"), ipaddr("4d93:5be:edbc:1830:344d:f71b:ce65:a4a3"), ipaddr("70ff:6bb4:a554:5af5:d90c:49f4:e8e6:eff0"), ipaddr("51b3:1bf0:1e65:740a:2b:51d9:162f:385a"), ipaddr("d6ea:3fcb:54bf:169f:9ab5:63bf:a960:19fb")], n)
	int128v = take([int128("7667974ea2fb155252559cc28b4a8efa"), int128("e7ef2788305d0f9c2c53cbfe3c373250"), int128("e602ccab7ff343e227b9596368ad5a44"), int128("709f888e885cfa716e0f36a0387477d5"), int128("978b68ce63f35ffbb79f23bd022269d8"), int128("022fd928ccbfc91efa6719ac22ccd239")], n)
	t = table(sym, boolv, intv, longv, shortv, doublev, floatv, str, charv, timestampv, datev, datetimev, monthv, timev, minutev, secondv, nanotimev, nanotimestampv, datehourv, uuidv, ipaddrv, int128v)
	pt.append!(t)`
	return dbPath, script
}

func TestDfsTable(t *testing.T) {
	t.Parallel()
	Convey("test dfsTable download data", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), host4, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer db.Close()
		var rowNum int
		Convey("test dfsTable only one rows", func() {
			rowNum = 1
			dbpath, s := CreateScript(rowNum)
			_, err = db.RunScript(s)
			So(err, ShouldBeNil)
			Convey("Test select single col from dfsTable:", func() {
				Convey("Test select bool col from dfsTable:", func() {
					s, err := db.RunScript("select boolv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reBool := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reBool.GetDataType(), ShouldEqual, model.DtBool)
					So(reBool.GetDataForm(), ShouldResemble, model.DfVector)
					So(reBool.Rows(), ShouldEqual, rowNum)
					re := reBool.Data.Value()
					tmp := []bool{true}
					for i := 0; i < reBool.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select int col from dfsTable:", func() {
					s, err := db.RunScript("select intv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reInt := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reInt.GetDataType(), ShouldEqual, model.DtInt)
					So(reInt.GetDataForm(), ShouldResemble, model.DfVector)
					So(reInt.Rows(), ShouldEqual, rowNum)
					re := reInt.Data.Value()
					tmp := []int32{91}
					for i := 0; i < reInt.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select long col from dfsTable:", func() {
					s, err := db.RunScript("select longv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reLong := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reLong.GetDataType(), ShouldEqual, model.DtLong)
					So(reLong.GetDataForm(), ShouldResemble, model.DfVector)
					So(reLong.Rows(), ShouldEqual, rowNum)
					re := reLong.Data.Value()
					tmp := []int64{99}
					for i := 0; i < reLong.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select short col from dfsTable:", func() {
					s, err := db.RunScript("select shortv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reShort := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reShort.GetDataType(), ShouldEqual, model.DtShort)
					So(reShort.GetDataForm(), ShouldResemble, model.DfVector)
					So(reShort.Rows(), ShouldEqual, rowNum)
					re := reShort.Data.Value()
					tmp := []int16{47}
					for i := 0; i < reShort.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select float col from dfsTable:", func() {
					s, err := db.RunScript("select floatv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reFloat := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reFloat.GetDataType(), ShouldEqual, model.DtFloat)
					So(reFloat.GetDataForm(), ShouldResemble, model.DfVector)
					So(reFloat.Rows(), ShouldEqual, rowNum)
					re := reFloat.Data.Value()
					tmp := []float32{5.2}
					for i := 0; i < reFloat.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select double col from dfsTable:", func() {
					s, err := db.RunScript("select doublev from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDouble := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDouble.GetDataType(), ShouldEqual, model.DtDouble)
					So(reDouble.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDouble.Rows(), ShouldEqual, rowNum)
					re := reDouble.Data.Value()
					tmp := []float64{4.7}
					for i := 0; i < reDouble.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select string col from dfsTable:", func() {
					s, err := db.RunScript("select str from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDouble := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDouble.GetDataType(), ShouldEqual, model.DtString)
					So(reDouble.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDouble.Rows(), ShouldEqual, rowNum)
					re := reDouble.Data.Value()
					tmp := []string{"str1"}
					for i := 0; i < reDouble.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select symbol col from dfsTable:", func() {
					s, err := db.RunScript("select sym from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDouble := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDouble.GetDataType(), ShouldEqual, model.DtSymbol)
					So(reDouble.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDouble.Rows(), ShouldEqual, rowNum)
					re := reDouble.Data.Value()
					tmp := []string{"A"}
					for i := 0; i < reDouble.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select char col from dfsTable:", func() {
					s, err := db.RunScript("select charv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDouble := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDouble.GetDataType(), ShouldEqual, model.DtChar)
					So(reDouble.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDouble.Rows(), ShouldEqual, rowNum)
					re := reDouble.Data.Value()
					tmp := []int8{70}
					for i := 0; i < reDouble.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select timestamp col from dfsTable:", func() {
					s, err := db.RunScript("select timestampv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reTimestamp := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reTimestamp.GetDataType(), ShouldEqual, model.DtTimestamp)
					So(reTimestamp.GetDataForm(), ShouldResemble, model.DfVector)
					So(reTimestamp.Rows(), ShouldEqual, rowNum)
					re := reTimestamp.Data.Value()
					timestampv := time.Date(2012, time.January, 01, 12, 23, 56, 166*1000000, time.UTC) //.Format("2006-01-02 15:04:05.166")
					tmp := []time.Time{timestampv}
					for i := 0; i < reTimestamp.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select datev col from dfsTable:", func() {
					s, err := db.RunScript("select datev from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDate := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDate.GetDataType(), ShouldEqual, model.DtDate)
					So(reDate.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDate.Rows(), ShouldEqual, rowNum)
					re := reDate.Data.IsNull(0)
					So(re, ShouldEqual, true)
				})
				Convey("Test select datetimev col from dfsTable:", func() {
					s, err := db.RunScript("select datetimev from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDatetime := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDatetime.GetDataType(), ShouldEqual, model.DtDatetime)
					So(reDatetime.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDatetime.Rows(), ShouldEqual, rowNum)
					re := reDatetime.Data.IsNull(0)
					So(re, ShouldEqual, true)
				})
				Convey("Test select month col from dfsTable:", func() {
					s, err := db.RunScript("select monthv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reMonth := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reMonth.GetDataType(), ShouldEqual, model.DtMonth)
					So(reMonth.GetDataForm(), ShouldResemble, model.DfVector)
					So(reMonth.Rows(), ShouldEqual, rowNum)
					re := reMonth.Data.Value()
					monthv := time.Date(1970, time.June, 01, 0, 0, 0, 0, time.UTC)
					tmp := []time.Time{monthv}
					for i := 0; i < reMonth.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select time col from dfsTable:", func() {
					s, err := db.RunScript("select timev from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reTime := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reTime.GetDataType(), ShouldEqual, model.DtTime)
					So(reTime.GetDataForm(), ShouldResemble, model.DfVector)
					So(reTime.Rows(), ShouldEqual, rowNum)
					re := reTime.Data.Value()
					timev := time.Date(1970, time.January, 01, 12, 23, 56, 156*1000000, time.UTC)
					tmp := []time.Time{timev}
					for i := 0; i < reTime.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select minute col from dfsTable:", func() {
					s, err := db.RunScript("select minutev from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reMinute := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reMinute.GetDataType(), ShouldEqual, model.DtMinute)
					So(reMinute.GetDataForm(), ShouldResemble, model.DfVector)
					So(reMinute.Rows(), ShouldEqual, rowNum)
					re := reMinute.Data.Value()
					minutev := time.Date(1970, time.January, 01, 12, 47, 0, 0, time.UTC)
					tmp := []time.Time{minutev}
					for i := 0; i < reMinute.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select second col from dfsTable:", func() {
					s, err := db.RunScript("select secondv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reSecond := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reSecond.GetDataType(), ShouldEqual, model.DtSecond)
					So(reSecond.GetDataForm(), ShouldResemble, model.DfVector)
					So(reSecond.Rows(), ShouldEqual, rowNum)
					re := reSecond.Data.IsNull(0)
					So(re, ShouldEqual, true)
				})
				Convey("Test select nanotime col from dfsTable:", func() {
					s, err := db.RunScript("select nanotimev from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reNanotime := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reNanotime.GetDataType(), ShouldEqual, model.DtNanoTime)
					So(reNanotime.GetDataForm(), ShouldResemble, model.DfVector)
					So(reNanotime.Rows(), ShouldEqual, rowNum)
					re := reNanotime.Data.Value()
					nanotimev := time.Date(1970, time.January, 01, 0, 0, 0, 000000001, time.UTC)
					tmp := []time.Time{nanotimev}
					for i := 0; i < reNanotime.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select nanotimestamp col from dfsTable:", func() {
					s, err := db.RunScript("select nanotimestampv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reNanotimestamp := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reNanotimestamp.GetDataType(), ShouldEqual, model.DtNanoTimestamp)
					So(reNanotimestamp.GetDataForm(), ShouldResemble, model.DfVector)
					So(reNanotimestamp.Rows(), ShouldEqual, rowNum)
					re := reNanotimestamp.Data.Value()
					nanotimestampv := time.Date(1969, time.December, 31, 23, 59, 59, 999999995, time.UTC)
					tmp := []time.Time{nanotimestampv}
					for i := 0; i < reNanotimestamp.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select datehour col from dfsTable:", func() {
					s, err := db.RunScript("select datehourv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDatehourv := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDatehourv.GetDataType(), ShouldEqual, model.DtDateHour)
					So(reDatehourv.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDatehourv.Rows(), ShouldEqual, rowNum)
					re := reDatehourv.Data.Value()
					datehourv := time.Date(1969, time.December, 01, 0, 0, 0, 0, time.UTC)
					tmp := []time.Time{datehourv}
					for i := 0; i < reDatehourv.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select uuid col from dfsTable:", func() {
					s, err := db.RunScript("select uuidv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reUUID := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reUUID.GetDataType(), ShouldEqual, model.DtUUID)
					So(reUUID.GetDataForm(), ShouldResemble, model.DfVector)
					So(reUUID.Rows(), ShouldEqual, rowNum)
					re := reUUID.Data.Value()
					tmp := []string{"7d943e7f-5660-e015-a895-fa4da2b36c43"}
					for i := 0; i < reUUID.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select ipaddr col from dfsTable:", func() {
					s, err := db.RunScript("select ipaddrv from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reIpaddr := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reIpaddr.GetDataType(), ShouldEqual, model.DtIP)
					So(reIpaddr.GetDataForm(), ShouldResemble, model.DfVector)
					So(reIpaddr.Rows(), ShouldEqual, rowNum)
					re := reIpaddr.Data.Value()
					tmp := []string{"a9b7:f65:9be1:20fd:741a:97ac:6ce5:1dd"}
					for i := 0; i < reIpaddr.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
				Convey("Test select int128 col from dfsTable:", func() {
					s, err := db.RunScript("select int128v from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reInt128 := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reInt128.GetDataType(), ShouldEqual, model.DtInt128)
					So(reInt128.GetDataForm(), ShouldResemble, model.DfVector)
					So(reInt128.Rows(), ShouldEqual, rowNum)
					re := reInt128.Data.Value()
					tmp := []string{"7667974ea2fb155252559cc28b4a8efa"}
					for i := 0; i < reInt128.Rows(); i++ {
						assert.Equal(t, re[i], tmp[i])
					}
				})
			})
			err = db.DropDatabase(&api.DropDatabaseRequest{Directory: dbpath})
			So(err, ShouldBeNil)
		})
		// Convey("test dfsTable less than 1024 rows", func() {
		// 	rowNum = 1023
		// 	_, err = db.RunScript(CreateScript(rowNum))
		// 	So(err, ShouldBeNil)
		// 	Convey("Test select bool col from dfsTable:", func() {
		// 		s, err := db.RunScript("select boolv from loadTable(dbName, `pt)")
		// 		So(err, ShouldBeNil)
		// 		memTable := s.(*model.Table)
		// 		reBool := memTable.GetColumn(memTable.GetColumnNames()[0])
		// 		So(reBool.GetDataType(), ShouldEqual, model.DtBool)
		// 		So(reBool.GetDataForm(), ShouldResemble, model.DfVector)
		// 		So(reBool.Rows(), ShouldEqual, rowNum)
		// 		re := reBool.Data.Value()
		// 		fmt.Printf("\nre %v", re)
		// 		tmp := []bool{true, false, true, false, false, true, true}
		// 		fmt.Printf("\ntmp %v", tmp)
		// 		var j int
		// 		for i := 0; i < reBool.Rows(); i++ {
		// 			if j < len(tmp) {
		// 				// assert.Equal(t, re[i], tmp[j])
		// 				So(re[i], ShouldEqual, tmp[j])
		// 				j += 1
		// 			} else {
		// 				j = 0
		// 				// assert.Equal(t, re[i], tmp[j])
		// 				So(re[i], ShouldEqual, tmp[j])
		// 				j += 1
		// 			}
		// 		}
		// 	})
		// })
	})
}

func CreateAllTypesDFSTableScript(Num int) (string, string) {
	dbName := "dfs://" + generateRandomString(5)
	s := `
	row_num=` + strconv.Itoa(Num) + `-1;
	cbool1 = bool(take(0 1 ,row_num));cchar1 = char(take(1..row_num ,row_num));cshort1 = short(take(1..row_num ,row_num));cint1 = int(take(1..row_num ,row_num));clong1 = long(take(1..row_num ,row_num));
	cdate1 = date(rand(10000 ,row_num));cmonth1 = month(rand(23640..25000 ,row_num));ctime1 = time(rand(10000 ,row_num));cminute1 = minute(rand(100 ,row_num));
	csecond1 = second(rand(100 ,row_num));cdatetime1 = datetime(rand(10000 ,row_num));ctimestamp1 = timestamp(rand(10000 ,row_num));cnanotime1 = nanotime(rand(10000 ,row_num));
	cnanotimestamp1 = nanotimestamp(rand(10000 ,row_num));cdatehour1 = datehour(rand(10000 ,row_num));cfloat1 = take(float(rand(1000.0000,2)) ,row_num);cdouble1 = take(rand(1000.0000,2) ,row_num);
	csymbol1 = take(symbol("a""s""sym""d""zdg""f""y""ddvb"),row_num);
	cstring1 = take("a""s""sym""d""zdg""f""y""ddvb",row_num);
	cblob1 = blob(string(take("a""s""sym""d""zdg""f""y""ddvb",row_num)));
	cipaddr1 =  take(ipaddr("192.168.1.13"),row_num);
	cuuid1 = take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),row_num);
	cint1281 = take(int128("e1671797c52e15f763380b45e841ec32"),row_num);
	ccomplex1 = take(complex(1 2 ,3 4), row_num);cpoint1 = take(point(1 2 ,3 4),row_num);
	cdecimal32_1 = decimal32(rand(1.0000, row_num),6);
	cdecimal64_1 = decimal64(rand(1.0000000, row_num),16);
	cdecimal128_1 =  decimal128(rand(1.00000000, row_num),26);


	ind = [2,4,6,8,10];
	cbool= arrayVector(ind, bool(rand(0 1 ,10)));cchar = arrayVector(ind, char(rand(256 ,10)));cshort = arrayVector(ind, short(rand(-10000..10000 ,10)));cint = arrayVector(ind, int(rand(-10000..10000 ,10)));
	clong = arrayVector(ind, long(rand(-10000..10000 ,10)));cdate = arrayVector(ind, date(rand(10000 ,10)));cmonth = arrayVector(ind, month(rand(23640..25000 ,10)));ctime = arrayVector(ind, time(rand(10000 ,10)));
	cminute = arrayVector(ind, minute(rand(100 ,10)));csecond = arrayVector(ind, second(rand(100 ,10)));cdatetime = arrayVector(ind, datetime(rand(10000 ,10)));ctimestamp = arrayVector(ind, timestamp(rand(10000 ,10)));
	cnanotime = arrayVector(ind, nanotime(rand(10000 ,10)));cnanotimestamp = arrayVector(ind, nanotimestamp(rand(10000 ,10)));cdatehour = arrayVector(ind, datehour(rand(10000 ,10)));
	cfloat = arrayVector(ind, float(rand(10000.0000,10)));cdouble = arrayVector(ind, rand(10000.0000,10));
	cdecimal32 = array(DECIMAL32(6)[], 0, 0).append!(decimal32([1..2, [], rand(100.000000, 2), rand(1..100, 2), take(00i, 2)], 6))
	cdecimal64 = array(DECIMAL64(16)[], 0, 0).append!(decimal64([1..2, [], rand(100.000000, 2), rand(1..100, 2), take(00i, 2)], 16))
	cdecimal128 = array(DECIMAL128(26)[], 0, 0).append!(decimal128([1..2, [], rand(100.000000, 2), rand(1..100, 2), take(00i, 2)], 26))
	cipaddr = arrayVector(ind, take(ipaddr(["192.168.1.13","192.168.1.14"]),10))
	cuuid = arrayVector(ind, take(uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88"]),10))
	cint128 = arrayVector(ind, take(int128(["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec33"]),10))

	for(i in 1..(row_num-5)){
		cbool.append!([bool(take(0 1 ,2))]);
		cchar.append!([char(rand(256 ,2))]);cshort.append!([short(rand(-10000..10000 ,2))]);cint.append!([int(rand(-10000..10000 ,2))]);
		clong.append!([long(rand(-10000..10000 ,2))]);cdate.append!([date(rand(10000 ,2))]);cmonth.append!([month(rand(23640..25000 ,2))]);
		ctime.append!([time(rand(10000 ,2))]);cminute.append!([minute(rand(100 ,2))]);csecond.append!([second(rand(100 ,2))]);
		cdatetime.append!([datetime(rand(10000 ,2))]);ctimestamp.append!([timestamp(rand(10000 ,2))]);
		cnanotime.append!([nanotime(rand(10000 ,2))]);cnanotimestamp.append!([nanotimestamp(rand(10000 ,2))]);
		cdatehour.append!([datehour(rand(10000 ,2))]);
		cfloat.append!([float(rand(10000.0000,2))]);cdouble.append!([rand(10000.0000, 2)]);
		cdecimal32.append!([decimal32("1.123123123123123123123123123"'-5.789' ,6)])
		cdecimal64.append!([decimal64("1.123123123123123123123123123"'-5.789' ,16)])
		cdecimal128.append!([decimal128("1.123123123123123123123123123"'-5.789' ,26)])
		cipaddr.append!([take(ipaddr(["192.168.1.13","192.168.1.14"]),2)])
		cuuid.append!([take(uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee88"]),2)])
		cint128.append!([take(int128(["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec33"]),2)])
	}

	go

	table3=table(cbool1,cchar1,cshort1,cint1,clong1,cdate1,cmonth1,ctime1,cminute1,csecond1,cdatetime1,ctimestamp1,cnanotime1,cnanotimestamp1,cdatehour1,cfloat1,cdouble1,csymbol1,cstring1,cblob1,cuuid1,cint1281,cipaddr1,ccomplex1,cpoint1,cdecimal32_1,cdecimal64_1,cdecimal128_1,
	cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cdatehour,cfloat,cdouble,cuuid,cint128,cipaddr,cdecimal32,cdecimal64,cdecimal128);

	tableInsert(table3, NULL,NULL,NULL,row_num+1,NULL,date(row_num+1),NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
	[take(bool(NULL),2)],[take(char(NULL),2)],[take(short(NULL),2)],[take(int(NULL),2)],[take(long(NULL),2)],[take(date(NULL),2)],[take(month(NULL),2)],[take(time(NULL),2)],[take(minute(NULL),2)],[take(second(NULL),2)],[take(datetime(NULL),2)],[take(timestamp(NULL),2)],[take(nanotime(NULL),2)],[take(nanotimestamp(NULL),2)],[take(datehour(NULL),2)],[take(float(NULL),2)],[take(double(NULL),2)],[take(uuid(string(NULL)),2)],[take(int128(string(NULL)),2)],[take(ipaddr(string(NULL)),2)],[take(decimal32(NULL,6),2)],[take(decimal64(NULL,16),2)],[take(decimal128(NULL,26),2)]);
	dbName = "` + dbName + `"
	tbname = "pt"
	if(existsDatabase(dbName))
		dropDatabase(dbName)
	db = database(dbName, HASH, [DATE, 2], engine="TSDB")
	db.createPartitionedTable(table3, tbname, "cdate1", , "cint1""cdate1").append!(table3)`
	return dbName, s
}

func TestDfsTable_decimal(t *testing.T) {
	Convey("test dfsTable download data", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), host4, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer db.Close()
		var rowNum int
		Convey("test dfsTable 100 rows", func() {
			rowNum = 100
			dbPath, s := CreateAllTypesDFSTableScript(rowNum)
			defer db.DropDatabase(&api.DropDatabaseRequest{Directory: dbPath})
			_, err = db.RunScript(s)
			So(err, ShouldBeNil)
			Convey("Test select single col from dfsTable:", func() {
				Convey("Test select decimal32v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal32_1 from loadTable('" + dbPath + "', `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal32v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal32v.GetDataType(), ShouldEqual, model.DtDecimal32)
					So(reDecimal32v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal32v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"res": reDecimal32v})
					ans, _ := db.RunScript(`ex = exec cdecimal32_1 from loadTable("` + dbPath + `", 'pt'); eqObj(ex, res)`)
					So(ans.(*model.Scalar).Value(), ShouldBeTrue)
				})
				Convey("Test select decimal64v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal64_1 from loadTable('" + dbPath + "', `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal64v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal64v.GetDataType(), ShouldEqual, model.DtDecimal64)
					So(reDecimal64v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal64v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"res": reDecimal64v})
					ans, _ := db.RunScript(`ex = exec cdecimal64_1 from loadTable("` + dbPath + `", 'pt'); eqObj(ex, res)`)
					So(ans.(*model.Scalar).Value(), ShouldBeTrue)
				})
			})
		})

		Convey("test dfsTable 65536 rows", func() {
			rowNum = 70000
			dbPath, s := CreateAllTypesDFSTableScript(rowNum)
			defer db.DropDatabase(&api.DropDatabaseRequest{Directory: dbPath})
			_, err = db.RunScript(s)
			So(err, ShouldBeNil)
			Convey("Test select single col from dfsTable:", func() {
				Convey("Test select decimal32v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal32_1 from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal32v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal32v.GetDataType(), ShouldEqual, model.DtDecimal32)
					So(reDecimal32v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal32v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"decimal32v": reDecimal32v})
					res, _ := db.RunScript(`ex = exec cdecimal32_1 from loadTable(dbName, 'pt');
											eqObj(ex, decimal32v)`)
					So(res.(*model.Scalar).Value(), ShouldBeTrue)
				})
				Convey("Test select decimal64v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal64_1 from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal64v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal64v.GetDataType(), ShouldEqual, model.DtDecimal64)
					So(reDecimal64v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal64v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"decimal64v": reDecimal64v})
					res, _ := db.RunScript(`ex = exec cdecimal64_1 from loadTable(dbName, 'pt');
											eqObj(ex, decimal64v)`)
					So(res.(*model.Scalar).Value(), ShouldBeTrue)
				})
			})
		})

	})
}

func TestDfsTable_decimal_arrayVector(t *testing.T) {
	Convey("test dfsTable download data", t, func() {
		db, err := api.NewSimpleDolphinDBClient(context.TODO(), host4, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		var rowNum int
		Convey("test dfsTable 100 rows", func() {
			rowNum = 100
			dbPath, s := CreateAllTypesDFSTableScript(rowNum)
			defer db.DropDatabase(&api.DropDatabaseRequest{Directory: dbPath})
			_, err = db.RunScript(s)
			So(err, ShouldBeNil)
			Convey("Test select single col from dfsTable:", func() {
				Convey("Test select decimal32v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal32 from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal32v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal32v.GetDataType(), ShouldEqual, model.DtDecimal32+64)
					So(reDecimal32v.GetDataTypeString(), ShouldEqual, "decimal32Array")
					So(reDecimal32v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal32v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"decimal32v": reDecimal32v})
					res, _ := db.RunScript(`ex = exec cdecimal32 from loadTable(dbName, 'pt');
											eqObj(ex, decimal32v)`)
					So(res.(*model.Scalar).Value(), ShouldBeTrue)
				})
				Convey("Test select decimal64v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal64 from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal64v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal64v.GetDataType(), ShouldEqual, model.DtDecimal64+64)
					So(reDecimal64v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal64v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"decimal64v": reDecimal64v})
					res, _ := db.RunScript(`ex = exec cdecimal64 from loadTable(dbName, 'pt');
											eqObj(ex, decimal64v)`)
					So(res.(*model.Scalar).Value(), ShouldBeTrue)
				})
			})
		})

		Convey("test dfsTable 65536 rows", func() {
			rowNum = 70000
			dbPath, s := CreateAllTypesDFSTableScript(rowNum)
			defer db.DropDatabase(&api.DropDatabaseRequest{Directory: dbPath})
			_, err = db.RunScript(s)
			So(err, ShouldBeNil)
			Convey("Test select single col from dfsTable:", func() {
				Convey("Test select decimal32v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal32 from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal32v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal32v.GetDataType(), ShouldEqual, model.DtDecimal32+64)
					So(reDecimal32v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal32v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"decimal32v": reDecimal32v})
					res, _ := db.RunScript(`ex = exec cdecimal32 from loadTable(dbName, 'pt');
											eqObj(ex, decimal32v)`)
					So(res.(*model.Scalar).Value(), ShouldBeTrue)
				})
				Convey("Test select decimal64v col from dfsTable:", func() {
					s, err := db.RunScript("select cdecimal64 from loadTable(dbName, `pt)")
					So(err, ShouldBeNil)
					memTable := s.(*model.Table)
					reDecimal64v := memTable.GetColumnByName(memTable.GetColumnNames()[0])
					So(reDecimal64v.GetDataType(), ShouldEqual, model.DtDecimal64+64)
					So(reDecimal64v.GetDataForm(), ShouldResemble, model.DfVector)
					So(reDecimal64v.Rows(), ShouldEqual, rowNum)
					db.Upload(map[string]model.DataForm{"decimal64v": reDecimal64v})
					res, _ := db.RunScript(`ex = exec cdecimal64 from loadTable(dbName, 'pt');
											eqObj(ex, decimal64v)`)
					So(res.(*model.Scalar).Value(), ShouldBeTrue)
				})
			})
		})

	})
}
