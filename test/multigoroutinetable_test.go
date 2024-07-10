package test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/model"
	mtw "github.com/dolphindb/api-go/multigoroutinetable"
	"github.com/dolphindb/api-go/test/setup"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	DBdfsPath     = "dfs://test_multiGoroutineTable"
	DBDiskPath    = setup.WORK_DIR + "/test_multiGoroutineTable"
	DfsTableName1 = "pt1"
	DfsTableName2 = "pt2"
)

var host12 = getRandomClusterAddress()
var waitGroup sync.WaitGroup

func CreateTimeList(n int, timeFomat string, timeList []string) []time.Time {
	ex := []time.Time{}
	for i := 0; i < len(timeList); i++ {
		timex, _ := time.Parse(timeFomat, timeList[i])
		ex = append(ex, timex)
	}
	return ex
}

func CheckListEqual(re []interface{}, ex []byte) bool {
	for i := 0; i < len(re); i++ {
		if re[i] != ex[i] {
			return false
		}
	}
	return true
}

type Tuple struct {
	boolcol       []bool
	charcol       []byte
	shortcol      []int16
	intcol        []int32
	longcol       []int64
	datecol       []time.Time
	monthcol      []time.Time
	timestampcol  []time.Time
	floatcol      []float32
	doublecol     []float64
	stringcol     []string
	symcol        []string
	uuidcol       []string
	int128col     []string
	ipaddrcol     []string
	decimal32col  []*model.Decimal32
	decimal64col  []*model.Decimal64
	decimal128col []*model.Decimal128
}

func insertalldatatype(mtt *mtw.MultiGoroutineTable) Tuple {
	timeList := []string{"1969/12/31 13:30:10.008", "1970/01/13 10:28:10.485", "2006/06/13 23:29:10.008", "1970/06/13 13:56:14.123", "1846/06/13 10:14:02.456", "2024/06/13 12:34:14.008"}
	colBool := []bool{true, false, true, false, false, false}
	colchar := []byte{2, 3, 4, 6, 5, 8}
	colshort := []int16{2, 3, 8, 10, 11, 15}
	colInt := []int32{2, 3, 8, 10, 11, 15}
	collong := []int64{2, 3, 8, 10, 11, 15}
	coldate := []time.Time{time.Date(2022, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, time.Month(12), 31, 0, 0, 0, 0, time.UTC),
		time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		time.Date(1971, time.Month(3), 12, 0, 0, 0, 0, time.UTC),
		time.Date(1969, time.Month(11), 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, time.Month(3), 1, 0, 0, 0, 0, time.UTC)}
	colmonthv := []time.Time{time.Date(2022, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, time.Month(12), 1, 0, 0, 0, 0, time.UTC),
		time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC),
		time.Date(1971, time.Month(3), 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, time.Month(11), 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, time.Month(3), 1, 0, 0, 0, 0, time.UTC)}
	coltimestamp := CreateTimeList(6, "15:04:05.041", timeList)
	colfloat := []float32{2.3, 4.6, 5.5, 4.9, 55.6, 22.3}
	coldouble := []float64{2.3, 4.6, 5.5, 4.9, 55.6, 22.3}
	colstring := []string{"AAPL", "AAPL", "GOOG", "GOOG", "MSFT", "MSFT", "IBM", "IBM", "YHOO", "YHOO"}
	colsym := []string{"AAPL", "AAPL", "GOOG", "GOOG", "MSFT", "MSFT", "IBM", "IBM", "YHOO", "YHOO"}
	coluuid := []string{"88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976", "88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976", "88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976"}
	colInt128 := []string{"af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484"}
	colipaddr := []string{"3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6"}
	coldecimal32 := []*model.Decimal32{
		{Scale: 6, Value: 10},
		{Scale: 6, Value: model.NullDecimal32Value},
		{Scale: 6, Value: 0},
		{Scale: 6, Value: -1.123},
		{Scale: 6, Value: 3.1234567},
		{Scale: 6, Value: model.NullDecimal32Value}}
	coldecimal64 := []*model.Decimal64{
		{Scale: 12, Value: 10},
		{Scale: 12, Value: model.NullDecimal64Value},
		{Scale: 12, Value: 0},
		{Scale: 12, Value: -1.123},
		{Scale: 12, Value: 3.1234567111111},
		{Scale: 12, Value: model.NullDecimal64Value}}
	coldecimal128 := []*model.Decimal128{
		{Scale: 24, Value: "10"},
		{Scale: 24, Value: model.NullDecimal128Value},
		{Scale: 24, Value: "0"},
		{Scale: 24, Value: "-1.123"},
		{Scale: 24, Value: "3.1234567111111111111111111"},
		{Scale: 24, Value: model.NullDecimal128Value}}

	for i := 0; i < 6; i++ {
		err := mtt.Insert(colBool[i], colchar[i], colshort[i], colInt[i], collong[i],
			coldate[i], colmonthv[i], coltimestamp[i], colfloat[i], coldouble[i], colstring[i], colsym[i], coluuid[i], colInt128[i], colipaddr[i], coldecimal32[i], coldecimal64[i], coldecimal128[i])
		if err != nil {
			panic(err)
		}
	}
	return Tuple{colBool, colchar, colshort, colInt, collong, coldate, colmonthv, coltimestamp, colfloat, coldouble, colstring, colsym, coluuid, colInt128, colipaddr, coldecimal32, coldecimal64, coldecimal128}
}

func threadinsertData(mtt *mtw.MultiGoroutineTable, n int) {
	i := 0
	for {
		err := mtt.Insert("AAPL"+strconv.Itoa(i%10),
			time.Date(1969, time.Month(12), i%10+1, 23, i%10, 50, 000, time.UTC),
			float64(22.5)+float64(i), float64(14.6)+float64(i), int32(i%10), float64(i))
		AssertNil(err)
		if err != nil {
			fmt.Println(err)
			break
		}
		if i == n-1 {
			break
		}
		i++
	}
	waitGroup.Done()
}

func insertDataTotable(n int, tableName string) {
	ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
	AssertNil(err)
	var symarr []string
	var datetimearr []time.Time
	var floatarr1 []float64
	var floatarr2 []float64
	var intarr []int32
	var floatarr3 []float64
	for i := 0; i < n; i++ {
		symarr = append(symarr, "AAPL"+strconv.Itoa(i%10))
		datetimearr = append(datetimearr, time.Date(1969, time.Month(12), i%10+1, 23, i%10, 50, 000, time.UTC))
		floatarr1 = append(floatarr1, float64(22.5)+float64(i))
		floatarr2 = append(floatarr2, float64(14.6)+float64(i))
		intarr = append(intarr, int32(i%10))
		floatarr3 = append(floatarr3, float64(i))
	}
	sym, _ := model.NewDataTypeListFromRawData(model.DtString, symarr)
	tradeDatev, _ := model.NewDataTypeListFromRawData(model.DtDatetime, datetimearr)
	tradePrice, _ := model.NewDataTypeListFromRawData(model.DtDouble, floatarr1)
	vwap, _ := model.NewDataTypeListFromRawData(model.DtDouble, floatarr2)
	volume, _ := model.NewDataTypeListFromRawData(model.DtInt, intarr)
	valueTrade, _ := model.NewDataTypeListFromRawData(model.DtDouble, floatarr3)
	tmp := model.NewTable([]string{"sym", "tradeDate", "tradePrice", "vwap", "volume", "valueTrade"},
		[]*model.Vector{model.NewVector(sym), model.NewVector(tradeDatev), model.NewVector(tradePrice),
			model.NewVector(vwap), model.NewVector(volume), model.NewVector(valueTrade)})
	_, err = ddb.RunFunc("tableInsert{"+tableName+"}", []model.DataForm{tmp})
	AssertNil(err)
	AssertNil(ddb.Close())
}

func TestMultiGoroutineTable_exception(t *testing.T) {
	Convey("test_multiGoroutineTable_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		Convey("Drop all Databases", func() {
			dbPaths := []string{DBdfsPath, DiskDBPath}
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
		Convey("test_multiGoroutineTable_exception", func() {
			Convey("test_multiGoroutineTable_error_hostName_exception", func() {
				scriptDFSHASH := `
				  if(existsDatabase("` + DBdfsPath + `")){
				    dropDatabase("` + DBdfsPath + `")
				  }
				  datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				  db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				  pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
					`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        "wrongHost" + strconv.Itoa(setup.Port),
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_error_port_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        setup.IP + ":-4",
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_error_userId_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         "dabsk",
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_error_password_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       "-2",
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_error_dbPath_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       "dhb",
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_dbPath_null_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       "",
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_error_TableName_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      "hsb",
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_TableName_null_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      "",
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_Throttle_less_than_0_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       -1,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_Throttle_0_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       0,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_BatchSize_equal_0_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      0,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_BatchSize_less_than_0_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      -1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_GoroutineCount_equal_0_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 0,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_GoroutineCount_less_than_0_exception", func() {
				scriptDFSHASH := `
					if(existsDatabase("` + DBdfsPath + `")){
						dropDatabase("` + DBdfsPath + `")
					}
					datetest=table(1000:0,["datev", "id"],[DATE,LONG])
					db=database("` + DBdfsPath + `",HASH, [MONTH,10])
					pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: -3,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_userid_no_grant_write_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				scriptusernograntwrite := `
				def test_user(){
					createUser("mark", "123456")
					grant("mark", TABLE_READ, "*")
				}
				rpc(getControllerAlias(),  test_user)
				`
				_, err = ddb.RunScript(scriptusernograntwrite)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         "mark",
					Password:       setup.Password,
					Address:        host12,
				}
				mtt, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldBeNil)
				err = mtt.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), int64(1))
				So(err, ShouldBeNil)
				mtt.WaitForGoroutineCompletion()
				errorInfo := mtt.GetStatus().ErrMsg
				So(errorInfo, ShouldResemble, "client error response. <NoPrivilege>Not granted to write data to table "+DBdfsPath+"/"+DfsTableName1)
				_, err = ddb.RunScript(`rpc(getControllerAlias(),  deleteUser,  "mark")`)
				So(err, ShouldBeNil)
				_, err = ddb.RunScript(`dropDatabase("` + DBdfsPath + `")`)
				So(err, ShouldBeNil)
			})
			Convey("test_multithreadTableWriterTest_Memory_Table_mutilthread_unspecified_partitioncolexception", func() {
				scriptMemoryTable := "t = table(1000:0, `id`x, [LONG, LONG]);share t as shareTable;"
				_, err = ddb.RunScript(scriptMemoryTable)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "",
					Database:       "",
					TableName:      "shareTable",
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
				_, err = ddb.RunScript("undef(`shareTable, SHARED)")
				So(err, ShouldBeNil)
			})
			Convey("test_multithreadTableWriterTest_DFS_Table_mutilthread_specified_not_partitioncolexception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "mt",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multithreadTableWriterTest_DFS_Table_partitioncolnull_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multithreadTableWriterTest_DFS_Table_GoroutineCount_>1_partitioncolnot_partitioncolumn_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 3,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "id",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
			})
			Convey("test_multithreadTableWriterTest_insert_different_data_type_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				mtt, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldBeNil)
				err = mtt.Insert("bjsa", int64(1))
				So(err, ShouldNotBeNil)
				mtt.WaitForGoroutineCompletion()
				So(err.Error(), ShouldResemble, "the type of in must be time.Time when datatype is DtDate")
				count, err := ddb.RunScript("exec count(*) from loadTable('" + DBdfsPath + "', '" + DfsTableName1 + "')")
				So(err, ShouldBeNil)
				dataNum := count.(*model.Scalar).Value()
				So(dataNum, ShouldEqual, 0)
				_, err = ddb.RunScript(`dropDatabase("` + DBdfsPath + `")`)
				So(err, ShouldBeNil)
			})
			Convey("test_multithreadTableWriterTest_Memory_Table_TableName_empty_exception", func() {
				scriptMemoryTable := "t = table(1000:0, `id`x, [LONG, LONG]);share t as shareTable;"
				_, err = ddb.RunScript(scriptMemoryTable)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "id",
					Database:       "",
					TableName:      "",
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
				_, err = ddb.RunScript("undef(`shareTable, SHARED)")
				So(err, ShouldBeNil)
			})
			Convey("test_multiGoroutineTable_insert_column_less_than_expected_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				mtt, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldBeNil)
				err = mtt.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC))
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_insert_null_rows_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				mtt, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldBeNil)
				err = mtt.Insert()
				So(err, ShouldNotBeNil)
			})
			Convey("test_multiGoroutineTable_insert_column_morethan_expected_exception", func() {
				scriptDFSHASH := `
				if(existsDatabase("` + DBdfsPath + `")){
					dropDatabase("` + DBdfsPath + `")
				}
				datetest=table(1000:0,["datev", "id"],[DATE,LONG])
				db=database("` + DBdfsPath + `",HASH, [MONTH,10])
				pt=db.createPartitionedTable(datetest,"` + DfsTableName1 + `",'datev')
				`
				_, err = ddb.RunScript(scriptDFSHASH)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "datev",
					Database:       DBdfsPath,
					TableName:      DfsTableName1,
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				mtt, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldBeNil)
				err = mtt.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), int64(10), int32(45))
				So(err, ShouldNotBeNil)
			})
			Convey("test_multithreadTableWriterTest_datatype_exception", func() {
				scriptGoroutineCount := "t = table(1000:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;"
				_, err = ddb.RunScript(scriptGoroutineCount)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 2,
					BatchSize:      1,
					Throttle:       1000,
					PartitionCol:   "id",
					Database:       "",
					TableName:      "t1",
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				mtt, err := mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldBeNil)
				tb := make([][]interface{}, 0)
				_tb := make([]interface{}, 0)
				c1 := make([]time.Time, 0)
				c2 := make([]int32, 0)
				c3 := make([]int32, 0)
				for i := 0; i < 3; i++ {
					dt1 := time.Date(2022, time.Month(1), i, 1, 1, 0, 0, time.UTC)
					c1 = append(c1, dt1)
					dt2 := int32(i)
					c2 = append(c2, dt2)
					dt3 := int32(16 + i)
					c3 = append(c3, dt3)
				}
				_tb = append(_tb, c1)
				_tb = append(_tb, c2)
				_tb = append(_tb, c3)
				tb = append(tb, _tb)
				err = mtt.InsertUnwrittenData(tb)
				So(err.Error(), ShouldContainSubstring, "col 1 of type symbol expect string slice")
				mtt.WaitForGoroutineCompletion()
				So(mtt.GetStatus().IsExit, ShouldBeTrue)
			})
			Convey("TestMultiGoroutineTable_insert_dfs_value_value_ex", func() {
				script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
					"if(exists(Database)){\n" +
					"\tdropDatabase(Database)\t\n" +
					"}\n" +
					"db1=database(\"\", VALUE, 1969.12.01..1969.12.10)\n" +
					"\tdb2=database(\"\", VALUE, 0..10)\n" +
					"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\")\n" +
					"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
					"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"volume\"])\n"
				_, err = ddb.RunScript(script)
				So(err, ShouldBeNil)
				opt := &mtw.Option{
					GoroutineCount: 1,
					BatchSize:      1000,
					Throttle:       20,
					PartitionCol:   "sym",
					Database:       "dfs://test_MultithreadedTableWriter",
					TableName:      "pt",
					UserID:         setup.UserName,
					Password:       setup.Password,
					Address:        host12,
				}
				_, err = mtw.NewMultiGoroutineTable(opt)
				So(err, ShouldNotBeNil)
				_, err = ddb.RunScript("undef(`t1, SHARED)")
				So(err, ShouldBeNil)
				_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestMultiGoroutineTable_all_data_type(t *testing.T) {
	Convey("test_multithreadTableWriterTest_all_data_type", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		scriptalldatatype := `
		t = table(1000:0, ["boolv", "charv", "shortv", "intv", "longv", "datev", "monthv", "timestampv", "floatv", "doublev", "stringv", "sym", "uuidv", "int128v", "ipv", "decimal32v", "decimal64v", "decimal128v"],
		[BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIMESTAMP, FLOAT, DOUBLE, STRING, SYMBOL, UUID, INT128, IPADDR, DECIMAL32(6), DECIMAL64(12), DECIMAL128(24)]);
		share t as all_data_type`
		_, err = ddb.RunScript(scriptalldatatype)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "all_data_type",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		tup := insertalldatatype(mtt)
		mtt.WaitForGoroutineCompletion()
		ErrMsg := mtt.GetStatus().ErrMsg
		So(ErrMsg, ShouldEqual, "")
		re, err := ddb.RunScript("select * from all_data_type order by intv")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		reColNameList := reTable.GetColumnNames()
		exColNameList := []string{"boolv", "charv", "shortv", "intv", "longv", "datev", "monthv", "timestampv", "floatv", "doublev", "stringv", "sym", "uuidv", "int128v", "ipv", "decimal32v", "decimal64v", "decimal128v"}
		So(reColNameList, ShouldResemble, exColNameList)
		reboolv := reTable.GetColumnByName("boolv").Data.Value()
		recharv := reTable.GetColumnByName("charv").Data.Value()
		reshortv := reTable.GetColumnByName("shortv").Data.Value()
		reintv := reTable.GetColumnByName("intv").Data.Value()
		relongv := reTable.GetColumnByName("longv").Data.Value()
		redatev := reTable.GetColumnByName("datev").Data.Value()
		remonthv := reTable.GetColumnByName("monthv").Data.Value()
		retimestampv := reTable.GetColumnByName("timestampv").Data.Value()
		refloatv := reTable.GetColumnByName("floatv").Data.Value()
		redoublev := reTable.GetColumnByName("doublev").Data.Value()
		restringv := reTable.GetColumnByName("stringv").Data.Value()
		resymv := reTable.GetColumnByName("sym").Data.Value()
		reuuidv := reTable.GetColumnByName("uuidv").Data.Value()
		reint128v := reTable.GetColumnByName("int128v").Data.Value()
		reipv := reTable.GetColumnByName("ipv").Data.Value()
		redecimal32v := reTable.GetColumnByName("decimal32v").Data.Value()
		redecimal64v := reTable.GetColumnByName("decimal64v").Data.Value()
		redecimal128v := reTable.GetColumnByName("decimal128v").Data.Value()
		for i := 0; i < reTable.Rows(); i++ {
			So(reboolv[i], ShouldEqual, tup.boolcol[i])
			So(recharv[i], ShouldEqual, tup.charcol[i])
			So(reshortv[i], ShouldEqual, tup.shortcol[i])
			So(reintv[i], ShouldEqual, tup.intcol[i])
			So(relongv[i], ShouldEqual, tup.longcol[i])
			So(redatev[i], ShouldEqual, tup.datecol[i])
			So(remonthv[i], ShouldEqual, tup.monthcol[i])
			So(retimestampv[i], ShouldEqual, tup.timestampcol[i])
			So(refloatv[i], ShouldEqual, tup.floatcol[i])
			So(redoublev[i], ShouldEqual, tup.doublecol[i])
			So(restringv[i], ShouldEqual, tup.stringcol[i])
			So(resymv[i], ShouldEqual, tup.symcol[i])
			So(reuuidv[i], ShouldEqual, tup.uuidcol[i])
			So(reint128v[i], ShouldEqual, tup.int128col[i])
			So(reipv[i], ShouldEqual, tup.ipaddrcol[i])
			coldecimal32 := []*model.Decimal32{
				{Scale: 6, Value: 10},
				{Scale: 6, Value: model.NullDecimal32Value},
				{Scale: 6, Value: 0},
				{Scale: 6, Value: -1.123},
				{Scale: 6, Value: 3.123456},
				{Scale: 6, Value: model.NullDecimal32Value}}
			coldecimal64 := []*model.Decimal64{
				{Scale: 12, Value: 10},
				{Scale: 12, Value: model.NullDecimal64Value},
				{Scale: 12, Value: 0},
				{Scale: 12, Value: -1.123},
				{Scale: 12, Value: 3.123456711111},
				{Scale: 12, Value: model.NullDecimal64Value}}
			coldecimal128 := []*model.Decimal128{
				{Scale: 24, Value: "10.000000000000000000000000"},
				{Scale: 24, Value: model.NullDecimal128Value},
				{Scale: 24, Value: "0.000000000000000000000000"},
				{Scale: 24, Value: "-1.123000000000000000000000"},
				{Scale: 24, Value: "3.123456711111111111111111"},
				{Scale: 24, Value: model.NullDecimal128Value}}
			So(redecimal32v[i], ShouldResemble, coldecimal32[i])
			So(redecimal64v[i], ShouldResemble, coldecimal64[i])
			So(redecimal128v[i], ShouldResemble, coldecimal128[i])
		}

		_, err = ddb.RunScript("undef(`all_data_type, SHARED)")
		So(err, ShouldBeNil)
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_GoroutineCount(t *testing.T) {
	Convey("test_multithreadTableWriterTest_GoroutineCount", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		s := "t = table(1:0, `date`id`values,[TIMESTAMP,SYMBOL,INT]);share t as t1;"
		_, err = ddb.RunScript(s)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 5,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "id",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		for i := 0; i < 100; i++ {
			err = mtt.Insert(time.Date(2022, time.Month(1), 1, 1, 1, 0, 0, time.UTC), "sym"+strconv.Itoa(i), int32(i))
			So(err, ShouldBeNil)
		}
		mtt.WaitForGoroutineCompletion()
		status := mtt.GetStatus()
		So(status.FailedRows, ShouldEqual, 0)
		So(status.ErrMsg, ShouldEqual, "")
		So(status.IsExit, ShouldBeTrue)
		So(len(status.GoroutineStatus), ShouldEqual, 5)
	})
}

func TestMultiGoroutineTable_null(t *testing.T) {
	Convey("test_multiGoroutineTable_prepare", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		Convey("test_multithreadTableWriterTest_insert_all_null", func() {
			scriptGoroutineCount := "t = table(1000:0, `boolv`charv`shortv`longv`datev`monthv`secondv`datetimev`timestampv`nanotimev`nanotimestampv`floatv`doublev`symbolv`stringv`uuidv`ipaddrv`int128v`intv`arrv`blobv," +
				"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,INT,BLOB]);" +
				"share t as t1;"
			_, err = ddb.RunScript(scriptGoroutineCount)
			So(err, ShouldBeNil)
			opt := &mtw.Option{
				GoroutineCount: 1,
				BatchSize:      1,
				Throttle:       1000,
				PartitionCol:   "boolv",
				Database:       "",
				TableName:      "t1",
				UserID:         setup.UserName,
				Password:       setup.Password,
				Address:        host12,
			}
			mtt, err := mtw.NewMultiGoroutineTable(opt)
			So(err, ShouldBeNil)
			err = mtt.Insert(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			So(err, ShouldBeNil)
			mtt.WaitForGoroutineCompletion()
			re, err := ddb.RunScript("select * from t1")
			So(err, ShouldBeNil)
			reTable := re.(*model.Table)
			So(reTable.Rows(), ShouldEqual, 1)
			_, err = ddb.RunScript("undef(`t1,SHARED)")
			So(err, ShouldBeNil)
		})

		Convey("test_multithreadTableWriterTest_insert_parted_null", func() {
			scriptGoroutineCount := "t = table(1000:0, `boolv`charv`shortv`longv`datev`monthv`secondv`datetimev`timestampv`nanotimev`nanotimestampv`floatv`doublev`symbolv`stringv`uuidv`ipaddrv`int128v`intv`arrv`blobv," +
				"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT,INT,BLOB]);" +
				"share t as t1;"
			_, err = ddb.RunScript(scriptGoroutineCount)
			So(err, ShouldBeNil)
			opt := &mtw.Option{
				GoroutineCount: 1,
				BatchSize:      1,
				Throttle:       1000,
				PartitionCol:   "boolv",
				Database:       "",
				TableName:      "t1",
				UserID:         setup.UserName,
				Password:       setup.Password,
				Address:        host12,
			}
			mtt, err := mtw.NewMultiGoroutineTable(opt)
			So(err, ShouldBeNil)
			err = mtt.Insert(nil, nil, int16(1), int64(4), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			So(err, ShouldBeNil)
			mtt.WaitForGoroutineCompletion()
			re, err := ddb.RunScript("select * from t1")
			So(err, ShouldBeNil)
			reTable := re.(*model.Table)
			So(reTable.Rows(), ShouldEqual, 1)
			_, err = ddb.RunScript("undef(`t1,SHARED)")
			So(err, ShouldBeNil)
		})
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_getStatus_write_successful(t *testing.T) {
	Convey("test_multithreadTableWriterTest_getStatus_write_successful", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		scriptGoroutineCount := "t = streamTable(1000:0, `intv`datev,[INT,DATE]);" + "share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "datev",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		for i := 0; i < 15; i++ {
			err = mtt.Insert(int32(i), time.Date(1969, time.Month(12), 20+i, 1, 1, 0, 0, time.UTC))
			So(err, ShouldBeNil)
		}
		mtt.WaitForGoroutineCompletion()
		status := mtt.GetStatus()
		So(status.FailedRows, ShouldEqual, 0)
		So(status.ErrMsg, ShouldEqual, "")
		So(status.IsExit, ShouldBeTrue)
		So(status.SentRows, ShouldEqual, 15)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}

func TestMultithreadTableWriterTest_getStatus_write_successful_normalData(t *testing.T) {
	Convey("test_multithreadTableWriterTest_getStatus_write_successful_normalData", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		scriptGoroutineCount := "t = streamTable(1000:0, `intv`datev,[INT,DATE]);" + "share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      100000,
			Throttle:       1000,
			PartitionCol:   "datev",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		for i := 0; i < 15; i++ {
			err = mtt.Insert(int32(i), time.Date(1969, time.Month(12), 20+i, 1, 1, 0, 0, time.UTC))
			So(err, ShouldBeNil)
		}
		status := mtt.GetStatus()
		So(status.FailedRows, ShouldEqual, 0)
		So(status.ErrMsg, ShouldEqual, "")
		So(status.IsExit, ShouldBeFalse)
		So(status.SentRows, ShouldEqual, 0)
		mtt.WaitForGoroutineCompletion()
		status = mtt.GetStatus()
		So(status.FailedRows, ShouldEqual, 0)
		So(status.ErrMsg, ShouldEqual, "")
		So(status.IsExit, ShouldBeTrue)
		So(status.SentRows, ShouldEqual, 15)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_bool(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_bool", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		scriptGoroutineCount := "t = streamTable(1000:0, `bool`id," +
			"[BOOL,INT]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "bool",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(byte(1), int32(16))
		So(err, ShouldBeNil)
		err = mtt.Insert(byte(0), int32(16))
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, int32(16))
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		So(reTable.GetColumnByName("id").String(), ShouldEqual, "vector<int>([16, 16, 16])")
		So(reTable.GetColumnByName("bool").Data.Value()[0], ShouldEqual, true)
		So(reTable.GetColumnByName("bool").Data.Value()[1], ShouldEqual, false)
		So(reTable.GetColumnByName("bool").Get(2).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}
func TestMultiGoroutineTable_insert_byte_int32_int64_int16(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_byte", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		scriptGoroutineCount := "t = streamTable(1000:0, `char`int`long`short`id," +
			"[CHAR,INT,LONG,SHORT,INT]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "id",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(byte(1), int32(1), int64(1), int16(1), int32(1))
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, int32(1), int64(1), int16(1), int32(1))
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		So(reTable.GetColumnByName("id").String(), ShouldEqual, "vector<int>([1, 1])")
		So(reTable.GetColumnByName("long").String(), ShouldEqual, "vector<long>([1, 1])")
		So(reTable.GetColumnByName("short").String(), ShouldEqual, "vector<short>([1, 1])")
		So(reTable.GetColumnByName("char").Data.Value()[0], ShouldEqual, 1)
		So(reTable.GetColumnByName("char").Get(1).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		err = ddb.Close()
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_float32_float64(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_double", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = streamTable(1000:0, `floatv`doublev`id," +
			"[FLOAT,DOUBLE,INT]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "id",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(float32(2.5), float64(5.6), int32(10))
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, nil, int32(1))
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		So(reTable.GetColumnByName("id").String(), ShouldEqual, "vector<int>([10, 1])")
		So(reTable.GetColumnByName("floatv").Data.Value()[0], ShouldEqual, float32(2.5))
		So(reTable.GetColumnByName("floatv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("doublev").Data.Value()[0], ShouldEqual, float64(5.6))
		So(reTable.GetColumnByName("doublev").Get(1).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_streamTable_insert_timetype(t *testing.T) {
	Convey("TestMultiGoroutineTable_streamTable_insert_timetype", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = streamTable(1000:0, `datev`monthv`secondv`minutev`datetimev`timestampv`datehourv`timev`nanotimev`nanotimestampv," +
			"[DATE, MONTH, SECOND, MINUTE, DATETIME, TIMESTAMP, DATEHOUR, TIME, NANOTIME, NANOTIMESTAMP]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      5,
			Throttle:       1000,
			PartitionCol:   "datev",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 144145868, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 144145868, time.UTC),
			time.Date(1969, time.Month(12), 31, 23, 59, 59, 144145868, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489548541, time.UTC),
			time.Date(1970, time.Month(1), 1, 12, 23, 0, 495321123, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC))
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			nil,
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 144145868, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489485541, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		So(reTable.GetColumnByName("datev").Get(0).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(0).String(), ShouldEqual, "1970.12M")
		So(reTable.GetColumnByName("secondv").Get(0).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(0).String(), ShouldEqual, "23:59m")
		So(reTable.GetColumnByName("datetimev").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(0).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(0).String(), ShouldEqual, "1970.01.01T12")
		So(reTable.GetColumnByName("timev").Get(0).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(0).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59.154140487")
		So(reTable.GetColumnByName("datev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("monthv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("minutev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datehourv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datev").Get(2).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(2).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(2).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(2).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(2).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(2).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(2).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_memTable_insert_timetype(t *testing.T) {
	Convey("TestMultiGoroutineTable_memTable_insert_timetype", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `datev`monthv`secondv`minutev`datetimev`timestampv`datehourv`timev`nanotimev`nanotimestampv," +
			"[DATE, MONTH, SECOND, MINUTE, DATETIME, TIMESTAMP, DATEHOUR, TIME, NANOTIME, NANOTIMESTAMP]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      5,
			Throttle:       1000,
			PartitionCol:   "datev",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC),
			time.Date(1969, time.Month(12), 31, 23, 59, 59, 489457541, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC),
			time.Date(1970, time.Month(1), 1, 12, 23, 0, 495321123, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC))
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			nil,
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		So(reTable.GetColumnByName("datev").Get(0).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(0).String(), ShouldEqual, "1970.12M")
		So(reTable.GetColumnByName("secondv").Get(0).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(0).String(), ShouldEqual, "23:59m")
		So(reTable.GetColumnByName("datetimev").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(0).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(0).String(), ShouldEqual, "1970.01.01T12")
		So(reTable.GetColumnByName("timev").Get(0).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(0).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59.154140487")
		So(reTable.GetColumnByName("datev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("monthv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("minutev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datehourv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datev").Get(2).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(2).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(2).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(2).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(2).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(2).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(2).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_dfsTable_insert_timetype(t *testing.T) {
	Convey("TestMultiGoroutineTable_dfsTable_insert_timetype", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptdfshashtable := `
		if(existsDatabase("` + DBdfsPath + `")){
			dropDatabase("` + DBdfsPath + `")
		}
		t=table(1000:0, ["datev", "monthv", "secondv", "minutev", "datetimev", "timestampv", "datehourv", "timev", "nanotimev", "nanotimestampv"], [DATE, MONTH, SECOND, MINUTE, DATETIME, TIMESTAMP, DATEHOUR, TIME, NANOTIME, NANOTIMESTAMP]);
		db=database("` + DBdfsPath + `", HASH, [MONTH, 10])
		pt=db.createPartitionedTable(t, "` + DfsTableName1 + `", 'datev')`
		_, err = ddb.RunScript(scriptdfshashtable)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      5,
			Throttle:       1000,
			PartitionCol:   "datev",
			Database:       DBdfsPath,
			TableName:      DfsTableName1,
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 145868, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 145868, time.UTC),
			time.Date(1969, time.Month(12), 31, 23, 59, 59, 112225671, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489541124, time.UTC),
			time.Date(1970, time.Month(1), 1, 12, 23, 0, 495321123, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC))
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), nil, time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC), nil, nil, nil, nil, nil, nil, nil)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			nil,
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 145485868, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489558941, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from loadTable('" + DBdfsPath + "', '" + DfsTableName1 + "')")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		So(reTable.GetColumnByName("datev").Get(0).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(0).String(), ShouldEqual, "1970.12M")
		So(reTable.GetColumnByName("secondv").Get(0).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(0).String(), ShouldEqual, "23:59m")
		So(reTable.GetColumnByName("datetimev").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(0).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(0).String(), ShouldEqual, "1970.01.01T12")
		So(reTable.GetColumnByName("timev").Get(0).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(0).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59.154140487")
		So(reTable.GetColumnByName("datev").Get(1).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(1).String(), ShouldEqual, "23:59:59")
		So(reTable.GetColumnByName("minutev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datehourv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datev").Get(2).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(2).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(2).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(2).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(2).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(2).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(2).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("dropDatabase('" + DBdfsPath + "')")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_dimensionTable_insert_timetype(t *testing.T) {
	Convey("TestMultiGoroutineTable_dimensionTable_insert_timetype", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptdfshashtable := `
		if(existsDatabase("` + DBdfsPath + `")){
			dropDatabase("` + DBdfsPath + `")
		}
		t = table(1000:0, ["datev", "monthv", "secondv", "minutev", "datetimev", "timestampv", "datehourv", "timev", "nanotimev", "nanotimestampv"], [DATE, MONTH, SECOND, MINUTE, DATETIME, TIMESTAMP, DATEHOUR, TIME, NANOTIME, NANOTIMESTAMP]);
		db=database("` + DBdfsPath + `", HASH, [MONTH, 10])
		pt=db.createTable(t, "` + DfsTableName1 + `")`
		_, err = ddb.RunScript(scriptdfshashtable)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      5,
			Throttle:       1000,
			PartitionCol:   "",
			Database:       DBdfsPath,
			TableName:      DfsTableName1,
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 145861458, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 145864878, time.UTC),
			time.Date(1969, time.Month(12), 31, 23, 59, 59, 111148745, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489541487, time.UTC),
			time.Date(1970, time.Month(1), 1, 12, 23, 0, 495321123, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC))
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC), nil, time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC), nil, nil, nil, nil, nil, nil, nil)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC),
			nil,
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 145887968, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489148541, time.UTC),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC),
			nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from loadTable('" + DBdfsPath + "', '" + DfsTableName1 + "')")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		So(reTable.GetColumnByName("datev").Get(0).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(0).String(), ShouldEqual, "1970.12M")
		So(reTable.GetColumnByName("secondv").Get(0).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(0).String(), ShouldEqual, "23:59m")
		So(reTable.GetColumnByName("datetimev").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(0).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(0).String(), ShouldEqual, "1970.01.01T12")
		So(reTable.GetColumnByName("timev").Get(0).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(0).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59.154140487")
		So(reTable.GetColumnByName("datev").Get(1).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(1).String(), ShouldEqual, "23:59:59")
		So(reTable.GetColumnByName("minutev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datehourv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datev").Get(2).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(2).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(2).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(2).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(2).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(2).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(2).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("dropDatabase('" + DBdfsPath + "')")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_memTable_insert_localTime(t *testing.T) {
	Convey("TestMultiGoroutineTable_memTable_insert_timetype", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `datev`monthv`secondv`minutev`datetimev`timestampv`datehourv`timev`nanotimev`nanotimestampv," +
			"[DATE, MONTH, SECOND, MINUTE, DATETIME, TIMESTAMP, DATEHOUR, TIME, NANOTIME, NANOTIMESTAMP]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      5,
			Throttle:       1000,
			PartitionCol:   "datev",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC).Local(),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC).Local(),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC).Local(),
			time.Date(1969, time.Month(12), 31, 23, 59, 59, 489457541, time.UTC).Local(),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local(),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC).Local(),
			time.Date(1970, time.Month(1), 1, 12, 23, 0, 495321123, time.UTC).Local(),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local(),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local(),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local())
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		So(err, ShouldBeNil)
		err = mtt.Insert(time.Date(1969, time.Month(12), 1, 1, 1, 0, 0, time.UTC).Local(),
			nil,
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC).Local(),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local(),
			time.Date(1970, time.Month(12), 1, 12, 23, 45, 489457541, time.UTC).Local(),
			nil,
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local(),
			time.Date(1968, time.Month(11), 1, 23, 59, 59, 154140487, time.UTC).Local(),
			nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		So(reTable.GetColumnByName("datev").Get(0).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(0).String(), ShouldEqual, "1970.12M")
		So(reTable.GetColumnByName("secondv").Get(0).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(0).String(), ShouldEqual, "23:59m")
		So(reTable.GetColumnByName("datetimev").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(0).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(0).String(), ShouldEqual, "1970.01.01T12")
		So(reTable.GetColumnByName("timev").Get(0).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(0).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(0).String(), ShouldEqual, "1968.11.01T23:59:59.154140487")
		So(reTable.GetColumnByName("datev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("monthv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("minutev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datehourv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimev").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("nanotimestampv").Get(1).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datev").Get(2).String(), ShouldEqual, "1969.12.01")
		So(reTable.GetColumnByName("monthv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("secondv").Get(2).String(), ShouldEqual, "12:23:45")
		So(reTable.GetColumnByName("minutev").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("datetimev").Get(2).String(), ShouldEqual, "1968.11.01T23:59:59")
		So(reTable.GetColumnByName("timestampv").Get(2).String(), ShouldEqual, "1970.12.01T12:23:45.489")
		So(reTable.GetColumnByName("datehourv").Get(2).IsNull(), ShouldEqual, true)
		So(reTable.GetColumnByName("timev").Get(2).String(), ShouldEqual, "23:59:59.154")
		So(reTable.GetColumnByName("nanotimev").Get(2).String(), ShouldEqual, "23:59:59.154140487")
		So(reTable.GetColumnByName("nanotimestampv").Get(2).IsNull(), ShouldEqual, true)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_part_null(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_part_null", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db  = database(Database, VALUE,`A`B`C`D);\n" + "t = table(1000:0, `boolv`charv`shortv`longv`datev`monthv`secondv`datetimev`timestampv`nanotimev`nanotimestampv`floatv`doublev`symbolv`stringv`uuidv`ipaddrv`int128v`id," +
			"[BOOL,CHAR,SHORT,LONG,DATE,MONTH,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,UUID, IPADDR, INT128,INT]);" +
			"pt = db.createTable(t,`pt);"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      5,
			Throttle:       1000,
			PartitionCol:   "boolv",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(byte(1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt);")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 1)
		unSentRows := mtt.GetStatus().UnSentRows
		So(unSentRows, ShouldEqual, 0)
		sentRows := mtt.GetStatus().SentRows
		So(sentRows, ShouldEqual, 1)
		So(reTable.GetColumnByName("boolv").String(), ShouldEqual, "vector<bool>([true])")
		So(reTable.GetColumnByName("charv").String(), ShouldEqual, "vector<char>([])")
		So(reTable.GetColumnByName("shortv").String(), ShouldEqual, "vector<short>([])")
		So(reTable.GetColumnByName("longv").String(), ShouldEqual, "vector<long>([])")
		So(reTable.GetColumnByName("datev").String(), ShouldEqual, "vector<date>([])")
		So(reTable.GetColumnByName("monthv").String(), ShouldEqual, "vector<month>([])")
		So(reTable.GetColumnByName("secondv").String(), ShouldEqual, "vector<second>([])")
		So(reTable.GetColumnByName("datetimev").String(), ShouldEqual, "vector<datetime>([])")
		So(reTable.GetColumnByName("timestampv").String(), ShouldEqual, "vector<timestamp>([])")
		So(reTable.GetColumnByName("nanotimev").String(), ShouldEqual, "vector<nanotime>([])")
		So(reTable.GetColumnByName("nanotimestampv").String(), ShouldEqual, "vector<nanotimestamp>([])")
		So(reTable.GetColumnByName("floatv").String(), ShouldEqual, "vector<float>([])")
		So(reTable.GetColumnByName("doublev").String(), ShouldEqual, "vector<double>([])")
		So(reTable.GetColumnByName("symbolv").String(), ShouldEqual, "vector<symbol>([])")
		So(reTable.GetColumnByName("stringv").String(), ShouldEqual, "vector<string>([])")
		So(reTable.GetColumnByName("uuidv").String(), ShouldEqual, "vector<uuid>([00000000-0000-0000-0000-000000000000])")
		So(reTable.GetColumnByName("ipaddrv").String(), ShouldEqual, "vector<ipaddr>([0.0.0.0])")
		So(reTable.GetColumnByName("int128v").String(), ShouldEqual, "vector<int128>([00000000000000000000000000000000])")
		So(reTable.GetColumnByName("id").String(), ShouldEqual, "vector<int>([])")
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_empty_arrayVector(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_empty_arrayVector", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`arrayv," +
			"[INT,INT[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int32{})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 1)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("arrayv")
		So(reIDv.String(), ShouldEqual, "vector<int>([10])")
		So(reArrayv.String(), ShouldEqual, "vector<intArray>([[]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_different_length(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_different_length", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`arrayv`arrayv1`arrayv2," +
			"[INT,INT[],BOOL[],BOOL[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int32{1, 3}, []byte{1, 0, model.NullBool}, []byte{1, 0})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int32{}, []byte{}, []byte{1, 0})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int32{1, 2, model.NullInt}, []byte{1, 0, model.NullBool}, []byte{1, 0})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 3)
		strmsg := mtt.GetStatus().String()
		So(strmsg, ShouldContainSubstring, "sentRows       :  3")
		So(reTable.GetColumnByName("intv").String(), ShouldEqual, "vector<int>([10, 10, 10])")
		So(reTable.GetColumnByName("arrayv").GetVectorValue(0).String(), ShouldEqual, "vector<int>([1, 3])")
		So(reTable.GetColumnByName("arrayv").GetVectorValue(1).String(), ShouldEqual, "vector<int>([])")
		So(reTable.GetColumnByName("arrayv").GetVectorValue(2).String(), ShouldEqual, "vector<int>([1, 2, ])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_char(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_char", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`charArr," +
			"[INT,CHAR[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []byte{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []byte{model.NullChar, 4})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("charArr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 10])")
		So(reArrayv.String(), ShouldEqual, "vector<charArray>([[], [, 4]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_int(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_int", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr," +
			"[INT,INT[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int32{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []int32{model.NullInt, 4})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("Arr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArrayv.String(), ShouldEqual, "vector<intArray>([[], [, 4]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_bool(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_bool", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr," +
			"[INT,BOOL[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []byte{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []byte{model.NullBool, 1})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("Arr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArrayv.String(), ShouldEqual, "vector<boolArray>([[], [, true]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_long(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_long", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr," +
			"[INT,LONG[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int64{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []int64{model.NullLong, 45})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		So(mtt.GetStatus().FailedRows, ShouldEqual, 0)
		So(mtt.GetStatus().UnSentRows, ShouldEqual, 0)
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("Arr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArrayv.String(), ShouldEqual, "vector<longArray>([[], [, 45]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_short(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_short", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr," +
			"[INT,SHORT[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []int16{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []int16{model.NullShort, 15})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		So(mtt.GetStatus().FailedRows, ShouldEqual, 0)
		So(mtt.GetStatus().UnSentRows, ShouldEqual, 0)
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("Arr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArrayv.String(), ShouldEqual, "vector<shortArray>([[], [, 15]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_float(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_float", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr," +
			"[INT,FLOAT[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []float32{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []float32{model.NullFloat, 2.6})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("Arr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArrayv.String(), ShouldEqual, "vector<floatArray>([[], [, 2.6]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_double(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_double", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr," +
			"[INT,DOUBLE[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []float64{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []float64{model.NullDouble, 2.6})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArrayv := reTable.GetColumnByName("Arr")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArrayv.String(), ShouldEqual, "vector<doubleArray>([[], [, 2.6]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_date_month(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_date_month", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr1`Arr2," +
			"[INT, DATE[], MONTH[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "Arr1",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []time.Time{}, []time.Time{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC), model.NullTime}, []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC)})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArray1v := reTable.GetColumnByName("Arr1")
		reArray2v := reTable.GetColumnByName("Arr2")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArray1v.String(), ShouldEqual, "vector<dateArray>([[], [, 1969.12.05, ]])")
		So(reArray2v.String(), ShouldEqual, "vector<monthArray>([[], [, 1969.12M]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_time_minute_month(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_time_minute_month", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr1`Arr2`Arr3," +
			"[INT, TIME[], MINUTE[], SECOND[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "Arr1",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []time.Time{}, []time.Time{}, []time.Time{})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC), model.NullTime}, []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC)}, []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC)})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArray1v := reTable.GetColumnByName("Arr1")
		reArray2v := reTable.GetColumnByName("Arr2")
		reArray3v := reTable.GetColumnByName("Arr3")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArray1v.String(), ShouldEqual, "vector<timeArray>([[], [, 23:56:59.456, ]])")
		So(reArray2v.String(), ShouldEqual, "vector<minuteArray>([[], [, 23:56m]])")
		So(reArray3v.String(), ShouldEqual, "vector<secondArray>([[], [, 23:56:59]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_datetime_timestamp_nanotime_nanotimstamp(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_datetime_timestamp_nanotime_nanotimstamp", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `intv`Arr1`Arr2`Arr3`Arr4," +
			"[INT, DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[]]);" +
			"share t as t1;"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "Arr1",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []time.Time{}, []time.Time{}, []time.Time{}, []time.Time{model.NullTime, time.Date(1970, time.Month(02), 5, 23, 56, 59, 999999999, time.UTC), model.NullTime})
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(3), []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC), model.NullTime}, []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC)}, []time.Time{model.NullTime, time.Date(1969, time.Month(12), 5, 23, 56, 59, 456789123, time.UTC)}, []time.Time{})
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		reIDv := reTable.GetColumnByName("intv")
		reArray1v := reTable.GetColumnByName("Arr1")
		reArray2v := reTable.GetColumnByName("Arr2")
		reArray3v := reTable.GetColumnByName("Arr3")
		reArray4v := reTable.GetColumnByName("Arr4")
		So(reIDv.String(), ShouldEqual, "vector<int>([10, 3])")
		So(reArray1v.String(), ShouldEqual, "vector<datetimeArray>([[], [, 1969.12.05T23:56:59, ]])")
		So(reArray2v.String(), ShouldEqual, "vector<timestampArray>([[], [, 1969.12.05T23:56:59.456]])")
		So(reArray3v.String(), ShouldEqual, "vector<nanotimeArray>([[], [, 23:56:59.456789123]])")
		So(reArray4v.String(), ShouldEqual, "vector<nanotimestampArray>([[, 1970.02.05T23:56:59.999999999, ], []])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_otherType(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_otherType", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = table(1000:0, `uuidv`int128v`ipaddrv," +
			"[UUID[],INT128[],IPADDR[]]);" +
			"share t as t1"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)

		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "uuidv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert([]string{"5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87", model.NullUUID}, []string{"e1671797c52e15f763380b45e841ec32", model.NullInt128, "e1671797c52e15f763380b45e841ec32"}, []string{"192.168.1.13", "192.168.1.84", model.NullIP})
		So(err, ShouldBeNil)
		err = mtt.Insert(nil, nil, nil)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		fmt.Println(reTable, reTable.Rows())
		So(reTable.Rows(), ShouldEqual, 2)
		reArray1v := reTable.GetColumnByName("uuidv")
		reArray2v := reTable.GetColumnByName("int128v")
		reArray3v := reTable.GetColumnByName("ipaddrv")
		So(reArray1v.String(), ShouldEqual, "vector<uuidArray>([[5d212a78-cc48-e3b1-4235-b4d91473ee87, 5d212a78-cc48-e3b1-4235-b4d91473ee87, 00000000-0000-0000-0000-000000000000], [00000000-0000-0000-0000-000000000000]])")
		So(reArray2v.String(), ShouldEqual, "vector<int128Array>([[e1671797c52e15f763380b45e841ec32, 00000000000000000000000000000000, e1671797c52e15f763380b45e841ec32], [00000000000000000000000000000000]])")
		So(reArray3v.String(), ShouldEqual, "vector<ipaddrArray>([[192.168.1.13, 192.168.1.84, 0.0.0.0], [0.0.0.0]])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_blob(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_blob", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		scriptGoroutineCount := "t = streamTable(1000:0, `intv`blobv," +
			"[INT, BLOB]);" +
			"share t as t1"
		_, err = ddb.RunScript(scriptGoroutineCount)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "intv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert(int32(10), []byte("aaaaadsfasdfaa"))
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 1)
		reArray1v := reTable.GetColumnByName("blobv")
		So(reArray1v.String(), ShouldEqual, "vector<blob>([aaaaadsfasdfaa])")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_arrayVector_wrong_type(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_arrayVector_wrong_type", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		s := "t = streamTable(1000:0, `intv`doublev," +
			"[INT[],DOUBLE[]]);" +
			"share t as t1"
		_, err = ddb.RunScript(s)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		err = mtt.Insert([]int32{1, 2, 3}, []float32{1.1, 2.2, 3.3})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "the type of input must be []float64 when datatype is DtDouble")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_uuid_int128_ipaddr(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_uuid_int128_ipaddr", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script1 := "t = streamTable(1000:0, `uuidv`ipaddrv`int128v," +
			"[UUID, IPADDR, INT128]);" +
			"share t as t1;"
		_, err = ddb.RunScript(script1)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1,
			Throttle:       1000,
			PartitionCol:   "uuidv",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		tb := make([][]interface{}, 0)
		_tb := make([]interface{}, 0)
		c1 := make([]string, 0)
		c2 := make([]string, 0)
		c3 := make([]string, 0)
		for i := 0; i < 3; i++ {
			dt1 := "00000000-0004-e72c-0000-000000007eb1"
			c1 = append(c1, dt1)
			dt2 := "192.168.100.20"
			c2 = append(c2, dt2)
			dt3 := "e1671797c52e15f763380b45e841ec32"
			c3 = append(c3, dt3)
		}
		_tb = append(_tb, c1)
		_tb = append(_tb, c2)
		_tb = append(_tb, c3)
		tb = append(tb, _tb)
		err = mtt.InsertUnwrittenData(tb)
		So(err, ShouldBeNil)
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1;")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.GetColumnByName("uuidv").String(), ShouldEqual, "vector<uuid>([00000000-0004-e72c-0000-000000007eb1, 00000000-0004-e72c-0000-000000007eb1, 00000000-0004-e72c-0000-000000007eb1])")
		So(reTable.GetColumnByName("int128v").String(), ShouldEqual, "vector<int128>([e1671797c52e15f763380b45e841ec32, e1671797c52e15f763380b45e841ec32, e1671797c52e15f763380b45e841ec32])")
		So(reTable.GetColumnByName("ipaddrv").String(), ShouldEqual, "vector<ipaddr>([192.168.100.20, 192.168.100.20, 192.168.100.20])")
		status := mtt.GetStatus()
		So(len(c1), ShouldEqual, status.UnSentRows+status.SentRows)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_keytable(t *testing.T) {
	Convey("TestMultiGoroutineTable_keytable", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "t=keyedStreamTable(`sym,1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade," +
			"[SYMBOL, DATETIME, DOUBLE, FLOAT, INT, DOUBLE])\n ;share t as t1;"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "",
			TableName:      "t1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		for i := 0; i < 10000; i++ {
			err = mtt.Insert("AAPL"+strconv.Itoa(i%2),
				time.Date(1969, time.Month(12), i%10+1, 23, i%10, 50, 000, time.UTC),
				float64(22.5)+float64(i), float32(14.6)+float32(i), int32(i%10), float64(i))
			AssertNil(err)
		}
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("select * from t1")
		So(err, ShouldBeNil)
		reTable := re.(*model.Table)
		So(reTable.Rows(), ShouldEqual, 2)
		status := mtt.GetStatus()
		So(status.SentRows, ShouldEqual, 10000)
		So(status.UnSentRows, ShouldEqual, 0)
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dt_multipleThreadCount(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dt_multipleThreadCount", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 2012.01.01..2012.01.30)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreateTable(dbHandle=db, table=t, tableName=`pt)\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		_, err = mtw.NewMultiGoroutineTable(opt)
		So(err.Error(), ShouldContainSubstring, "the parameter GoroutineCount must be 1 for a dimension table")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_tsdb_dt_multipleThreadCount(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_tsdb_dt_multipleThreadCount", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 2012.01.01..2012.01.30,,'TSDB')\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreateTable(dbHandle=db, table=t, tableName=`pt,sortColumns=`sym)\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		_, err = mtw.NewMultiGoroutineTable(opt)
		So(err.Error(), ShouldContainSubstring, "the parameter GoroutineCount must be 1 for a dimension table")
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}
func TestMultiGoroutineTable_insert_dt_multipleThread_groutine(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dt_multipleThread_groutine", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 2012.01.01..2012.01.30)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreateTable(dbHandle=db, table=t, tableName=`pt)\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dt_multipleThread_tsdb_groutine(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dt_multipleThread_tsdb_groutine", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 2012.01.01..2012.01.30,,'TSDB')\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreateTable(dbHandle=db, table=t, tableName=`pt, sortColumns=`sym)\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dt_oneThread(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dt_oneThread", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 2012.01.01..2012.01.30)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreateTable(dbHandle=db, table=t, tableName=`pt)\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 1
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_value(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_value", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, month(2012.01.01)+0..1)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_hash(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_hash", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, HASH, [SYMBOL,3])\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"sym\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       2,
			PartitionCol:   "sym",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1,SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_list(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_list", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, LIST, [`AAPL0`AAPL1`AAPL2, `AAPL3`AAPL4`AAPL5, `AAPL6`AAPL7`AAPL8`AAPL9])\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n ;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"sym\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      10,
			Throttle:       20,
			PartitionCol:   "sym",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_value_value(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_value_value", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", VALUE, 1969.12.01..1969.12.10)\n" +
			"\tdb2=database(\"\", VALUE, 0..10)\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"volume\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_value_range(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_value_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", VALUE, 1969.12.01..1969.12.10)\n" +
			"\tdb2=database(\"\", RANGE,0 5 10)\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"volume\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_range_value(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_value_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", VALUE, 1969.12.01..1969.12.10)\n" +
			"\tdb2=database(\"\", RANGE,0 5 10)\n" +
			"\tdb=database(Database, COMPO, [db2, db1], , \"OLAP\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\", \"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_range_range(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_range_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", RANGE, 1969.12.01 1969.12.05 1969.12.11)\n" +
			"\tdb2=database(\"\", RANGE,0 5 11)\n" +
			"\tdb=database(Database, COMPO, [db2, db1], , \"OLAP\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\", \"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_range_hash(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_range_hash", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", RANGE, 1969.12.01 1969.12.05 1969.12.11)\n" +
			"\tdb2=database(\"\", HASH,[INT,3])\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\", \"volume\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_hash_range(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_hash_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", RANGE, 1969.12.01 1969.12.05 1969.12.11)\n" +
			"\tdb2=database(\"\", HASH,[INT,3])\n" +
			"\tdb=database(Database, COMPO, [db2, db1], , \"OLAP\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\", \"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_hash_hash_chunkGranularity_database(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_hash_hash", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", HASH, [DATEHOUR,3])\n" +
			"\tdb2=database(\"\", HASH,[SYMBOL,3])\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE]);\n share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"sym\"],compressMethods={tradeDate:\"delta\", volume:\"delta\"})\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_hash_value_chunkGranularity_database(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_hash_value_chunkGranularity_database", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", HASH, [DATEHOUR,3])\n" +
			"\tdb2=database(\"\", VALUE, 0..10)\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"volume\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_hash_range_chunkGranularity_database(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_dfs_hash_value", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", HASH, [DATEHOUR,3])\n" +
			"\tdb2=database(\"\", RANGE, 0 5 11)\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"volume\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_PartitionType_datehour_partirtioncoldatetime(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_PartitionType_datehour_partitioncoldatetime", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", VALUE, date(1969.12.01)+0..10)\n" +
			"\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"sym\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_PartitionType_datehour_partitioncoltimestamp(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_PartitionType_datehour_partitioncoltimestamp", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", VALUE, date(1969.12.01)+0..10)\n" +
			"\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"sym\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_PartitionType_datehour_partitioncolnanotimestamp(t *testing.T) {
	Convey("TestMultiGoroutineTable_insert_PartitionType_datehour_partitioncolnanotimestamp", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", VALUE, date(1969.12.01)+0..10)\n" +
			"\tdb2=database(\"\", HASH, [SYMBOL, 2])\n" +
			"\tdb=database(Database, COMPO, [db1, db2], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\",\"sym\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoldatetime(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoldatetime", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, datehour(1969.12.01)+0..10)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoltimestamp(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoltimestamp", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, datehour(1969.12.01)+0..10)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncolnanotimestamp(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncolnanotimestamp", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, datehour(1969.12.01)+0..10)\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoldate_range(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoldate_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, RANGE, [1969.12.01, 1969.12.05, 1969.12.11])\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoldatetime_range(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoldatetime_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, RANGE, [1969.12.01, 1969.12.05, 1969.12.11])\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoltimestamp_range(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncoltimestamp_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, RANGE, [1969.12.01, 1969.12.05, 1969.12.11])\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncolnanotimestamp_range(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_date_partitioncolnanotimestamp", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, RANGE, [1969.12.01, 1969.12.05, 1969.12.11])\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, NANOTIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_month_partitioncoldatetime_range(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_month_partitioncoldatetime_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, month(1..10))\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_month_partitioncoltimestamp_range(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_PartitionType_partitiontype_month_partitioncoltimestamp_range", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, month(1..10))\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      1000,
			Throttle:       20,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		threadTime := 10
		n := 1000
		waitGroup.Add(threadTime)
		for i := 0; i < threadTime; i++ {
			go threadinsertData(mtt, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_multiple_mutithreadTableWriter_sameTable(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_multiple_mutithreadTableWriter_sameTable", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "\n" +
			"Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 1..5)\n" +
			"t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n" +
			" ;share t as t1;\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      100000,
			Throttle:       100,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		mtt2, err := mtw.NewMultiGoroutineTable(opt)
		So(err, ShouldBeNil)
		tb1 := make([][]interface{}, 0)
		tb2 := make([][]interface{}, 0)
		_tb1 := make([]interface{}, 0)
		_tb2 := make([]interface{}, 0)
		c11 := make([]int32, 0)
		c12 := make([]float64, 0)
		c21 := make([]int32, 0)
		c22 := make([]float64, 0)
		for i := 0; i < 1; i++ {
			dt1 := int32(1)
			c11 = append(c11, dt1)
			dt2 := float64(12.9)
			c12 = append(c12, dt2)
			dt3 := int32(2)
			c21 = append(c21, dt3)
			dt4 := float64(22.9)
			c22 = append(c22, dt4)
		}
		_tb1 = append(_tb1, c11)
		_tb1 = append(_tb1, c12)
		_tb2 = append(_tb2, c21)
		_tb2 = append(_tb2, c22)
		tb1 = append(tb1, _tb1)
		tb2 = append(tb2, _tb2)
		for i := 0; i < 10; i++ {
			err = mtt1.InsertUnwrittenData(tb1)
			AssertNil(err)
			err = mtt2.InsertUnwrittenData(tb2)
			AssertNil(err)
		}
		for j := 0; j < 10; j++ {
			var intarr1 []int32
			var floatarr1 []float64
			for i := 0; i < 1; i++ {
				floatarr1 = append(floatarr1, float64(12.9))
				intarr1 = append(intarr1, int32(1))
			}
			valueTrade1, _ := model.NewDataTypeListFromRawData(model.DtDouble, floatarr1)
			volume1, _ := model.NewDataTypeListFromRawData(model.DtInt, intarr1)
			tmp1 := model.NewTable([]string{"volume", "valueTrade"},
				[]*model.Vector{model.NewVector(volume1), model.NewVector(valueTrade1)})
			_, err = ddb.RunFunc("tableInsert{t1}", []model.DataForm{tmp1})
			AssertNil(err)
			time.Sleep(3 * time.Second)
			var intarr2 []int32
			var floatarr2 []float64
			for i := 0; i < 1; i++ {
				floatarr2 = append(floatarr2, float64(22.9))
				intarr2 = append(intarr2, int32(2))
			}
			valueTrade2, _ := model.NewDataTypeListFromRawData(model.DtDouble, floatarr2)
			volume2, _ := model.NewDataTypeListFromRawData(model.DtInt, intarr2)
			tmp2 := model.NewTable([]string{"volume", "valueTrade"},
				[]*model.Vector{model.NewVector(volume2), model.NewVector(valueTrade2)})
			_, err = ddb.RunFunc("tableInsert{t1}", []model.DataForm{tmp2})
			AssertNil(err)
		}
		mtt1.WaitForGoroutineCompletion()
		mtt2.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from t1 order by volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, reTable2.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_multiple_mutithreadTableWriter_differentTable(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_multiple_mutithreadTableWriter_differentTable", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "\n" +
			"Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 1..5)\n" +
			"t=table(1:0, `volume`valueTrade, [INT, DOUBLE])\n;share t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"]);\n" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt2, partitionColumns=[\"volume\"]);\n" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt3, partitionColumns=[\"volume\"]);\n" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt4, partitionColumns=[\"volume\"]);\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		opt2 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      30,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt2",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt2, err := mtw.NewMultiGoroutineTable(opt2)
		So(err, ShouldBeNil)
		opt3 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      100,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt3",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt3, err := mtw.NewMultiGoroutineTable(opt3)
		So(err, ShouldBeNil)
		opt4 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt4",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt4, err := mtw.NewMultiGoroutineTable(opt4)
		So(err, ShouldBeNil)
		tb := make([][]interface{}, 0)
		_tb := make([]interface{}, 0)
		c1 := make([]int32, 0)
		c2 := make([]float64, 0)
		for i := 0; i < 1; i++ {
			dt1 := int32(16 + i)
			c1 = append(c1, dt1)
			dt2 := float64(22.9)
			c2 = append(c2, dt2)
		}
		_tb = append(_tb, c1)
		_tb = append(_tb, c2)
		tb = append(tb, _tb)
		for i := 0; i < 10; i++ {
			err = mtt1.InsertUnwrittenData(tb)
			AssertNil(err)
			err = mtt2.InsertUnwrittenData(tb)
			AssertNil(err)
			err = mtt3.InsertUnwrittenData(tb)
			AssertNil(err)
			err = mtt4.InsertUnwrittenData(tb)
			AssertNil(err)
		}
		for j := 0; j < 10; j++ {
			var intarr []int32
			var floatarr1 []float64
			for i := 0; i < 1; i++ {
				floatarr1 = append(floatarr1, float64(22.9))
				intarr = append(intarr, int32(16))
			}
			valueTrade, _ := model.NewDataTypeListFromRawData(model.DtDouble, floatarr1)
			volume, _ := model.NewDataTypeListFromRawData(model.DtInt, intarr)
			tmp := model.NewTable([]string{"volume", "valueTrade"},
				[]*model.Vector{model.NewVector(volume), model.NewVector(valueTrade)})
			_, err = ddb.RunFunc("tableInsert{t1}", []model.DataForm{tmp})
			AssertNil(err)
		}
		mtt1.WaitForGoroutineCompletion()
		mtt2.WaitForGoroutineCompletion()
		mtt3.WaitForGoroutineCompletion()
		mtt4.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt1) order by volume,valueTrade")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt2) order by volume,valueTrade")
		So(err, ShouldBeNil)
		re3, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt3) order by volume,valueTrade")
		So(err, ShouldBeNil)
		re4, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt4) order by volume,valueTrade")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by volume,valueTrade")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		reTable3 := re3.(*model.Table)
		reTable4 := re4.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
			So(reTable2.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
			So(reTable3.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
			So(reTable4.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}
func TestMultiGoroutineTable_insert_tsdb_keepDuplicates(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_tsdb_keepDuplicates", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "\n" +
			"Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, 0..11,,'TSDB');\n" +
			//"share keyedStreamTable(`volume`tradeDate,1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, DOUBLE, DOUBLE, INT, DOUBLE]) as t1\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATE, DOUBLE, DOUBLE, INT, DOUBLE]); share t as t1; " +
			"createPartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"volume\"],sortColumns=`volume`tradeDate,compressMethods={volume:\"delta\"},keepDuplicates=LAST);" +
			"createPartitionedTable(dbHandle=db, table=t, tableName=`pt2, partitionColumns=[\"volume\"],sortColumns=`volume`tradeDate,keepDuplicates=FIRST);" +
			"createPartitionedTable(dbHandle=db, table=t, tableName=`pt3, partitionColumns=[\"volume\"],sortColumns=`volume`tradeDate,keepDuplicates=LAST);" +
			"createTable(dbHandle=db, table=t, tableName=`pt4, sortColumns=`volume`tradeDate,compressMethods={volume:\"delta\"},keepDuplicates=LAST);\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      1000,
			Throttle:       100,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		opt2 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      30,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt2",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt2, err := mtw.NewMultiGoroutineTable(opt2)
		So(err, ShouldBeNil)
		opt3 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      100,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt3",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt3, err := mtw.NewMultiGoroutineTable(opt3)
		So(err, ShouldBeNil)
		opt4 := &mtw.Option{
			GoroutineCount: 1,
			BatchSize:      100,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt4",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt4, err := mtw.NewMultiGoroutineTable(opt4)
		So(err, ShouldBeNil)
		n := 100
		waitGroup.Add(40)
		for i := 0; i < 10; i++ {
			go threadinsertData(mtt1, n)
			go threadinsertData(mtt2, n)
			go threadinsertData(mtt3, n)
			go threadinsertData(mtt4, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		mtt2.WaitForGoroutineCompletion()
		mtt3.WaitForGoroutineCompletion()
		mtt4.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt1) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		re2, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt2) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		re3, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt3) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		re4, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt4) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex1, err := ddb.RunScript("select * from t1 where isDuplicated([volume, tradeDate], LAST)=false order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex2, err := ddb.RunScript("select * from t1 where isDuplicated([volume, tradeDate], FIRST)=false order by sym,tradePrice,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		reTable2 := re2.(*model.Table)
		reTable3 := re3.(*model.Table)
		reTable4 := re4.(*model.Table)
		exTable1 := ex1.(*model.Table)
		exTable2 := ex2.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable1.GetColumnByIndex(i).String())
			So(reTable2.GetColumnByIndex(i).String(), ShouldEqual, exTable2.GetColumnByIndex(i).String())
			So(reTable3.GetColumnByIndex(i).String(), ShouldEqual, exTable1.GetColumnByIndex(i).String())
			So(reTable4.GetColumnByIndex(i).String(), ShouldEqual, exTable1.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_length_eq_1024(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_length_eq_1024", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", RANGE, 1969.12.01+(0..11))\n" +
			"\tdb2=database(\"\", HASH,[INT,3])\n" +
			"\tdb=database(Database, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\nshare t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 1024
		waitGroup.Add(1)
		for i := 0; i < 1; i++ {
			go threadinsertData(mtt1, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_length_eq_1048576(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_length_eq_1048576", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", RANGE, 1969.12.01+(0..11))\n" +
			"\tdb2=database(\"\", HASH,[INT,3])\n" +
			"\tdb=database(Database, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\nshare t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 1048576
		waitGroup.Add(1)
		for i := 0; i < 1; i++ {
			go threadinsertData(mtt1, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfs_length_eq_3000000(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfs_length_eq_3000000", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db1=database(\"\", RANGE, 1969.12.01+(0..11))\n" +
			"\tdb2=database(\"\", HASH,[INT,3])\n" +
			"\tdb=database(Database, COMPO, [db2, db1], , \"OLAP\", chunkGranularity=\"DATABASE\")\n" +
			"t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL,DATEHOUR, DOUBLE, DOUBLE, INT, DOUBLE])\nshare t as t1;" +
			"\tcreatePartitionedTable(dbHandle=db, table=t, tableName=`pt, partitionColumns=[\"volume\",\"tradeDate\"])\n"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10000,
			Throttle:       100,
			PartitionCol:   "volume",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 3000000
		waitGroup.Add(1)
		for i := 0; i < 1; i++ {
			threadinsertData(mtt1, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt) order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_streamTable_multipleThread(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_streamTable_multipleThread", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		t1 := "t1_" + generateRandomString(5)
		t2 := "t2_" + generateRandomString(5)
		defer ddb.Close()
		script := "t=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as " + t1 + ";" +
			"tt=table(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as " + t2 + ";"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10,
			Throttle:       3,
			PartitionCol:   "volume",
			Database:       "",
			TableName:      t2,
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 1000
		waitGroup.Add(10)
		for i := 0; i < 10; i++ {
			go threadinsertData(mtt1, n)
			insertDataTotable(n, t1)
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from " + t2 + " order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from " + t1 + " order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`" + t1 + "`" + t2 + ", SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_streamtable_200cols(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_streamtable_200cols", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "t=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR])\n;\n" +
			"addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;" +
			"tt=streamTable(1:0, `sym`tradeDate, [SYMBOL,DATEHOUR])\n;" +
			"addColumn(tt,\"col\"+string(1..200),take([DOUBLE],200));share tt as trades;"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      10000,
			Throttle:       1000,
			PartitionCol:   "sym",
			Database:       "",
			TableName:      "trades",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		for ind := 0; ind < 10; ind++ {
			row := make([]model.DataForm, 202)
			dt, err := model.NewDataType(model.DtString, "AAPL")
			AssertNil(err)
			row[0] = model.NewScalar(dt)
			dt, err = model.NewDataType(model.DtNanoTimestamp, time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC))
			AssertNil(err)
			row[1] = model.NewScalar(dt)
			i := float64(ind)
			for j := 0; j < 200; j++ {
				dt, err = model.NewDataType(model.DtDouble, i+0.1)
				AssertNil(err)
				row[j+2] = model.NewScalar(dt)
			}
			_, err = ddb.RunFunc("tableInsert{t1}", row)
			So(err, ShouldBeNil)
			err = mtt1.Insert("AAPL", time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC), i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1)
			AssertNil(err)
		}
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from trades order by sym,tradeDate;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("undef(`trades, SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_dfstable_200cols(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_dfstable_200cols", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "t=table(1:0, `sym`tradeDate, [SYMBOL,TIMESTAMP]);\n" +
			"addColumn(t,\"col\"+string(1..200),take([DOUBLE],200));share t as t1;" +
			"Database = \"dfs://test_MultithreadedTableWriter\"\n" +
			"if(exists(Database)){\n" +
			"\tdropDatabase(Database)\t\n" +
			"}\n" +
			"db=database(Database, VALUE, date(1..2),,'TSDB');\n" +
			"createPartitionedTable(dbHandle=db, table=t, tableName=`pt1, partitionColumns=[\"tradeDate\"],sortColumns=`sym,compressMethods={tradeDate:\"delta\"});"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      1000,
			Throttle:       1000,
			PartitionCol:   "tradeDate",
			Database:       "dfs://test_MultithreadedTableWriter",
			TableName:      "pt1",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		for ind := 0; ind < 10; ind++ {
			row := make([]model.DataForm, 202)
			dt, err := model.NewDataType(model.DtString, "AAPL")
			AssertNil(err)
			row[0] = model.NewScalar(dt)
			dt, err = model.NewDataType(model.DtNanoTimestamp, time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC))
			AssertNil(err)
			row[1] = model.NewScalar(dt)
			i := float64(ind)
			for j := 0; j < 200; j++ {
				dt, err = model.NewDataType(model.DtDouble, i+0.1)
				AssertNil(err)
				row[j+2] = model.NewScalar(dt)
			}
			_, err = ddb.RunFunc("tableInsert{t1}", row)
			So(err, ShouldBeNil)
			err = mtt1.Insert("AAPL", time.Date(2022, time.Month(1), 1+ind%10, 1, 1, 0, 0, time.UTC), i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1,
				i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1, i+0.1)
			AssertNil(err)
		}
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from loadTable('dfs://test_MultithreadedTableWriter',`pt1) order by sym,tradeDate;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_MultithreadedTableWriter\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_concurrentWrite_getFailedData_when_unfinished_write(t *testing.T) {
	Convey("func TestMultiGoroutineTable_concurrentWrite_getFailedData_when_unfinished_write", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "login(`admin,`123456)\n" +
			"Database = \"dfs://test_mtw_concurrentWrite_FailedData\"\n" +
			"if(existsDatabase(Database)){\n" +
			"\tdropDB(Database)\n" +
			"}\n" +
			"db = database(Database,RANGE,0 10 20 30)\n" +
			"t = table(10:0,`id`price`val,[INT,DOUBLE,INT])\n" +
			"pt = db.createPartitionedTable(t,`pt,`id)"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      1000,
			Throttle:       1000,
			PartitionCol:   "id",
			Database:       "dfs://test_mtw_concurrentWrite_FailedData",
			TableName:      "pt",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		for i := 0; i < 10000; i++ {
			err = mtt.Insert(int32(5), float64(14.6), int32(1))
			AssertNil(err)
		}
		failedData := mtt.GetStatus().FailedRows
		UnwrittenData := mtt.GetUnwrittenData()
		mtt.WaitForGoroutineCompletion()
		re, err := ddb.RunScript("(exec count(*) from loadTable(Database, `pt) where val = 1)[0]")
		So(err, ShouldBeNil)
		reTable := re.(*model.Scalar)
		unwrittenLength := 0
		for _, v := range UnwrittenData {
			unwrittenLength += len(v[0].([]int32))
			// unwrittenLength += len(v[1].([]float64))
			// unwrittenLength += len(v[2].([]int32))
		}
		So(failedData+unwrittenLength+int(reTable.Value().(int32)), ShouldEqual, 10000)
		_, err = ddb.RunScript("dropDatabase(\"dfs://test_mtw_concurrentWrite_FailedData\")")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_streamTable_eq_1024(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_streamTable_eq_1024", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as t2;"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      100000,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "",
			TableName:      "t2",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 1024
		waitGroup.Add(1)
		for i := 0; i < 1; i++ {
			go threadinsertData(mtt1, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from t2 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_streamTable_eq_1048576(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_streamTable_eq_1048576", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as t2;"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      100000,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "",
			TableName:      "t2",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 1048576
		waitGroup.Add(1)
		for i := 0; i < 1; i++ {
			go threadinsertData(mtt1, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from t2 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("undef(`t2, SHARED)")
		So(err, ShouldBeNil)
	})
}

func TestMultiGoroutineTable_insert_streamTable_eq_3000000(t *testing.T) {
	Convey("func TestMultiGoroutineTable_insert_streamTable_eq_3000000", t, func() {
		ddb, err := api.NewSimpleDolphinDBClient(context.TODO(), host12, setup.UserName, setup.Password)
		So(err, ShouldBeNil)
		defer ddb.Close()
		script := "t=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share t as t1;" +
			"tt=streamTable(1:0, `sym`tradeDate`tradePrice`vwap`volume`valueTrade, [SYMBOL, DATETIME, DOUBLE, DOUBLE, INT, DOUBLE])\n;share tt as t2;"
		_, err = ddb.RunScript(script)
		So(err, ShouldBeNil)
		opt1 := &mtw.Option{
			GoroutineCount: 2,
			BatchSize:      100000,
			Throttle:       1000,
			PartitionCol:   "volume",
			Database:       "",
			TableName:      "t2",
			UserID:         setup.UserName,
			Password:       setup.Password,
			Address:        host12,
		}
		mtt1, err := mtw.NewMultiGoroutineTable(opt1)
		So(err, ShouldBeNil)
		n := 3000000
		waitGroup.Add(1)
		for i := 0; i < 1; i++ {
			go threadinsertData(mtt1, n)
			insertDataTotable(n, "t1")
		}
		waitGroup.Wait()
		mtt1.WaitForGoroutineCompletion()
		re1, err := ddb.RunScript("select * from t2 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		ex, err := ddb.RunScript("select * from t1 order by sym,tradeDate,tradePrice,vwap,volume,valueTrade;")
		So(err, ShouldBeNil)
		reTable1 := re1.(*model.Table)
		exTable := ex.(*model.Table)
		for i := 0; i < len(reTable1.GetColumnNames()); i++ {
			So(reTable1.GetColumnByIndex(i).String(), ShouldEqual, exTable.GetColumnByIndex(i).String())
		}
		_, err = ddb.RunScript("undef(`t1, SHARED)")
		So(err, ShouldBeNil)
		_, err = ddb.RunScript("undef(`t2, SHARED)")
		So(err, ShouldBeNil)
	})
}
