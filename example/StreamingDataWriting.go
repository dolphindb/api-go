package main

import (
	"../api"
	"fmt"
	"runtime"
	"time"
)

const (
	host     = "localhost"
	port     = 8848
	username = "admin"
	password = "123456"
)

func CreateDemoTable(rows int, startp byte, pcount byte, starttime int, timeInc int) ddb.Table {
	colnames := []string{"id", "cbool", "cchar", "cshort", "cint",
		"clong", "cdate", "cmonth", "ctime", "cminute",
		"csecond", "cdatetime", "ctimestamp", "cnanotime", "cnanotimestamp",
		"cfloat", "cdouble", "csymbol", "cstring", "cuuid",
		"cip", "cint128"}

	coltypes := []int{
		ddb.DT_LONG, ddb.DT_BOOL, ddb.DT_CHAR, ddb.DT_SHORT, ddb.DT_INT,
		ddb.DT_LONG, ddb.DT_DATE, ddb.DT_MONTH, ddb.DT_TIME, ddb.DT_MINUTE,
		ddb.DT_SECOND, ddb.DT_DATETIME, ddb.DT_TIMESTAMP, ddb.DT_NANOTIME, ddb.DT_NANOTIMESTAMP,
		ddb.DT_FLOAT, ddb.DT_DOUBLE, ddb.DT_SYMBOL, ddb.DT_STRING, ddb.DT_UUID,
		ddb.DT_IP, ddb.DT_INT128}
	colnum := 22
	rownum := rows
	table := ddb.CreateTable(colnames, coltypes, rownum, rows)

	colv := [22]ddb.Vector{}
	for i := 0; i < colnum; i++ {
		colv[i] = table.GetColumn(i)
	}

	ip := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	for i := 0; i < rows; i++ {
		colv[0].SetLongByIndex(i, int64(i))
		colv[1].SetBoolByIndex(i, true)
		colv[2].SetBoolByIndex(i, false)
		colv[3].SetShortByIndex(i, int16(i))
		colv[4].SetIntByIndex(i, int32(i))
		colv[5].SetLongByIndex(i, int64(i))
		colv[6].SetByIndex(i, ddb.ParseConstant(ddb.DT_DATE, "2020.01.01"))
		colv[7].SetIntByIndex(i, 24240) // 2020.01M
		colv[8].SetIntByIndex(i, int32(i))
		colv[9].SetIntByIndex(i, int32(i))
		colv[10].SetIntByIndex(i, int32(i))
		colv[11].SetIntByIndex(i, int32(1577836800+i))     // 2020.01.01 00:00:00+i
		colv[12].SetLongByIndex(i, int64(1577836800000+i)) // 2020.01.01 00:00:00+i
		colv[13].SetLongByIndex(i, int64(i))
		colv[14].SetLongByIndex(i, int64(1577836800000000000+i)) // 2020.01.01 00:00:00.000000000+i
		colv[15].SetFloatByIndex(i, float32(i))
		colv[16].SetDoubleByIndex(i, float64(i))
		colv[17].SetStringByIndex(i, "sym")
		colv[18].SetStringByIndex(i, "abc")
		ip[15] = byte(i)
		colv[19].SetBinaryByIndex(i, ip)
		colv[20].SetBinaryByIndex(i, ip)
		colv[21].SetBinaryByIndex(i, ip)
	}
	return table
}

func finsert(rows int, startp byte, pcount byte, starttime int, timeInc int, host string, port int, p int, inserttimes int) {
	t1 := time.Now()
	var conn ddb.DBConnection
	conn.Init()
	success := conn.Connect(host, port, username, password)
	if !success {
		panic("connect failed!")
	}
	t := CreateDemoTable(rows, startp, pcount, starttime, timeInc)
	tb := t.ToConstant()
	args := []ddb.Constant{tb}

	for i := 0; i < inserttimes; i++ {
		conn.RunFunc("tableInsert{objByName(`st1)}", args)
		runtime.Gosched()
	}
	fmt.Println("insert", rows, "rows", inserttimes, "times, cost", time.Since(t1))
}

func main() {
	tablerows := 100
	inserttimes := 10
	for i := 0; i < 5; i++ {
		finsert(tablerows, byte(i*5-1), byte(5), int(ddb.GetEpochTime()/1000), i*5, host, port, i, inserttimes)
	}
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(host, port, username, password)
	res := conn.Run("login(`admin, `123456); select count(*) from objByName(`st1)")
	fmt.Println(res.GetString())

}
