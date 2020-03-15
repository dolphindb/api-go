package main

import (
	"../src"
	"fmt"
	"runtime"
	"strconv"
	"time"
)

const (
	username = "admin"
	password = "123456"
)

func CreateDemoTable(rows int, startp byte, pcount byte, starttime int, timeInc int) ddb.Table {
	colnames := []string{"fwname",
		"filename",
		"source_address",
		"source_port",
		"destination_address",
		"destination_port",
		"nat_source_address",
		"nat_source_port",
		"starttime",
		"stoptime",
		"elapsed_time"}

	coltypes := []int{ddb.DT_SYMBOL, ddb.DT_STRING, ddb.DT_IP, ddb.DT_INT, ddb.DT_IP, ddb.DT_INT, ddb.DT_IP, ddb.DT_INT, ddb.DT_DATETIME, ddb.DT_DATETIME, ddb.DT_INT}
	colnum := 11
	rownum := rows
	table := ddb.CreateTable(colnames, coltypes, rownum, rows)

	colv := [11]ddb.Vector{}
	for i := 0; i < colnum; i++ {
		colv[i] = table.GetColumn(i)
	}

	sip := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	ip := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	sip[3] = 192
	sip[2] = startp
	sip[1] = pcount

	spIP := ddb.CreateConstant(ddb.DT_IP)
	for j := 1; j < 255; j++ {
		sip[0] = byte(j)
		spIP.SetBinary(sip)
		x := byte(spIP.GetHash(50))
		if x >= startp && x < startp+pcount {
			break
		}
	}

	for i := 0; i < rows; i++ {

		colv[0].SetStringByIndex(i, "10.189.45.2:9000")
		colv[1].SetStringByIndex(i, strconv.Itoa(int(startp)))
		colv[2].SetBinaryByIndex(i, sip)

		colv[3].SetIntByIndex(i, int32(1*i))
		colv[4].SetBinaryByIndex(i, ip)
		colv[5].SetIntByIndex(i, int32(2*i))

		colv[6].SetByIndex(i, ddb.ParseConstant(ddb.DT_IP, "192.168.1.1"))
		colv[7].SetIntByIndex(i, int32(3*i))
		colv[8].SetLongByIndex(i, int64(starttime+timeInc))

		colv[9].SetLongByIndex(i, int64(starttime+100))
		colv[10].SetIntByIndex(i, int32(i))
	}
	return table
}

var quit chan int = make(chan int)

func finsert(rows int, startp byte, pcount byte, starttime int, timeInc int, hosts []string, ports []int, p int, inserttimes int, c chan int) {
	t1 := time.Now()
	var conn ddb.DBConnection
	conn.Init()
	success := conn.Connect(hosts[p], ports[p], username, password)
	if !success {
		panic("connect failed!")
	}
	t := CreateDemoTable(rows, startp, pcount, starttime, timeInc)
	tb := t.ToConstant()
	args := []ddb.Constant{tb}

	for i := 0; i < inserttimes; i++ {
		conn.RunFunc("tableInsert{loadTable('dfs://natlog', `natlogrecords)}", args)
		runtime.Gosched()
	}
	fmt.Println("insert", rows, "rows", inserttimes, "times, cost", time.Since(t1))
	c <- 1
}

func main() {
	runtime.GOMAXPROCS(10)
	hosts := []string{"localhost", "localhost", "localhost", "localhost", "localhost"}
	ports := []int{1321, 1322, 1323, 1324, 1325}
	lh := len(hosts)
	if lh != len(ports) {
		panic("Hosts and ports should have equal length !")
	}
	if lh > 10 {
		panic("Hosts should be fewer than  10 !")
	}

	c := make(chan int, lh)
	tablerows := 10000
	inserttimes := 100

	for i := 0; i < lh; i++ {
		go finsert(tablerows, byte(i*5-1), byte(5), int(ddb.GetEpochTime()/1000), i*5, hosts, ports, i, inserttimes, c)
	}

	for i := 0; i < lh; i++ {
		<-c
	}

	fmt.Println("end")
}
