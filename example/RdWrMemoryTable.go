package main

import (
	"../src"
	"fmt"
	"strconv"
	"time"
)

const (
	host     = "127.0.0.1"
	port     = 8848
	username = "admin"
	password = "123456"
)

func main() {
	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(host, port, username, password)
	script := "kt = keyedTable(`col_int, 2000:0, `col_int`col_short`col_long`col_float`col_double`col_bool`col_string,  [INT, SHORT, LONG, FLOAT, DOUBLE, BOOL, STRING]); "
	conn.Run(script)

	t := time.Now()

	coltypes := []int{ddb.DT_INT, ddb.DT_SHORT, ddb.DT_LONG, ddb.DT_FLOAT, ddb.DT_DOUBLE, ddb.DT_BOOL, ddb.DT_STRING}
	colnum := 7
	rownum := 10000
	colv := [11]ddb.Vector{}
	for i := 0; i < colnum; i++ {
		colv[i] = ddb.CreateVector(coltypes[i], 0)
	}
	v0 := []int32{}
	v1 := []int16{}
	v2 := []int64{}
	v3 := []float32{}
	v4 := []float64{}
	v5 := []bool{}
	v6 := []string{}
	for i := 0; i < rownum; i++ {
		v0 = append(v0, int32(i))
		v1 = append(v1, 255)
		v2 = append(v2, 10000)
		v3 = append(v3, 133.3)
		v4 = append(v4, 255.0)
		v5 = append(v5, true)
		v6 = append(v6, "str")
	}
	colv[0].AppendInt(v0, rownum)
	colv[1].AppendShort(v1, rownum)
	colv[2].AppendLong(v2, rownum)
	colv[3].AppendFloat(v3, rownum)
	colv[4].AppendDouble(v4, rownum)
	colv[5].AppendBool(v5, rownum)
	colv[6].AppendString(v6, rownum)

	args := []ddb.Constant{colv[0].ToConstant(), colv[1].ToConstant(), colv[2].ToConstant(), colv[3].ToConstant(), colv[4].ToConstant(), colv[5].ToConstant(), colv[6].ToConstant()}
	script2 := "tableInsert{kt}"
	conn.RunFunc(script2, args)
	t1 := time.Since(t)
	fmt.Println("tableInsert cost ", t1)

	t = time.Now()
	for i := rownum; i < rownum*2; i++ {
		scripti := fmt.Sprintf("insert into kt values(%s, 255, 10000,  133.3, 255.0, true, 'str' );", strconv.Itoa(int(i)))
		conn.Run(scripti)
	}
	t1 = time.Since(t)
	fmt.Println("sql 10000 times cost ", t1)

	re2 := conn.Run("select count(*) from kt")
	fmt.Println(re2.GetString())

	res := conn.Run("select * from kt")
	res_table := res.ToTable()

	fmt.Println(res_table.Rows(), " rows ")
	fmt.Println(res_table.Columns(), " columns ")
	fmt.Println()

	re1 := conn.Run("select  top 5 * from kt")
	fmt.Println(re1.GetString())

	re3 := conn.Run("select * from kt where col_int =30")
	fmt.Println(re3.GetString())
}
