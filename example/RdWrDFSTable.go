package main

import (
	"../src"
	"fmt"
)

const (
	host = "127.0.0.1"
	port = 8848
	user = "admin"
	pass = "123456"
)

func main() {

	var conn ddb.DBConnection
	conn.Init()
	conn.Connect(host, port, user, pass)
	script := "t = table(100:0, `id`date`x , [INT, DATE, DOUBLE]); share t as tglobal;"
	script += "login(`admin, `123456); dbPath='dfs://datedb'; if(existsDatabase(dbPath))\ndropDatabase(dbPath); db=database(dbPath, VALUE, 2017.08.07..2017.08.11); tb=db.createPartitionedTable(t, `pt,`date)"
	conn.Run(script)
	rownum := 1000

	v1 := ddb.CreateVector(ddb.DT_INT, 0)
	v2 := ddb.CreateVector(ddb.DT_DATE, 0)
	v3 := ddb.CreateVector(ddb.DT_DOUBLE, 0)
	for i := 0; i < rownum; i++ {
		v1.Append(ddb.CreateInt(i))
		v2.Append(ddb.CreateDate(2017, 8, 7+i%5))
		v3.Append(ddb.CreateDouble(3.1415926))
	}
	cols := []ddb.Vector{v1, v2, v3}
	colnames := []string{"id", "date", "x"}
	t := ddb.CreateTableByVector(colnames, cols)

	args := []ddb.Constant{t.ToConstant()}
	conn.RunFunc("tableInsert{loadTable('dfs://datedb', `pt)}", args)

	res := conn.Run("select count(*) from loadTable('dfs://datedb', `pt)")
	fmt.Println(res.GetString())
}
