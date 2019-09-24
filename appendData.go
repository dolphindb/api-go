package main

import (
   "./api"
   "fmt"
   "time"
)

const(
	host = "localhost";
	port = 1321;
	user = "admin";
	pass = "123456";
)

func CreateDemoTable() ddb.Table{
  rowNum :=100000;
  v1 := ddb.CreateVector(ddb.DT_BOOL);
  // v2 := ddb.CreateVector(ddb.DT_CAHR);
  v3 := ddb.CreateVector(ddb.DT_SHORT);
  v4 := ddb.CreateVector(ddb.DT_INT);
  v5 := ddb.CreateVector(ddb.DT_LONG);
  v6 := ddb.CreateVector(ddb.DT_DATE);
  v7 := ddb.CreateVector(ddb.DT_MONTH);
  v8 := ddb.CreateVector(ddb.DT_TIME);
  v9 := ddb.CreateVector(ddb.DT_MINUTE);
  v10 := ddb.CreateVector(ddb.DT_SECOND);
  v11 := ddb.CreateVector(ddb.DT_DATETIME);
  v12 := ddb.CreateVector(ddb.DT_TIMESTAMP);
  v13 := ddb.CreateVector(ddb.DT_NANOTIME);
  v14 := ddb.CreateVector(ddb.DT_NANOTIMESTAMP);
  v15 := ddb.CreateVector(ddb.DT_FLOAT);
  v16 := ddb.CreateVector(ddb.DT_DOUBLE);
  // v17 := ddb.CreateVector(ddb.DT_SYMBOL);
  v18 := ddb.CreateVector(ddb.DT_STRING);

  va1 := ddb.CreateBool(true);
  // va2 := ddb.CreateChar(1);
  va3 := ddb.CreateShort(1);
  va4 := ddb.CreateInt(1);
  va5 := ddb.CreateLong(1);
  va6 := ddb.CreateDate(2019, 1, 1);
  va7 := ddb.CreateMonth(2019, 1);
  va8 := ddb.CreateTime(13, 30, 36, 500);
  va9 := ddb.CreateMinute(13, 30);
  va10 := ddb.CreateSecond(13, 30, 36);
  va11 := ddb.CreateDateTime(2019, 1, 1, 13, 30, 36);
  va12 := ddb.CreateTimestamp(2019, 1, 1, 13, 30, 36, 500);
  va13 := ddb.CreateNanoTime(13, 30, 36, 0500000);
  va14 := ddb.CreateNanoTimestamp(2019, 1, 1, 13, 30, 36, 0500000);
  va15 := ddb.CreateFloat(1.0);
  va16 := ddb.CreateDouble(1.0);
  // va17 := ddb.CreateSymbol("1");
  va18 := ddb.CreateString("1");

  for i :=0; i<rowNum; i++{
    v1.Append(va1);
    // v2 := ddb.CreateVector(va2);
    v3.Append(va3);
    v4.Append(va4);
    v5.Append(va5);
    v6.Append(va6);
    v7.Append(va7);
    v8.Append(va8);
    v9.Append(va9);
    v10.Append(va10);
    v11.Append(va11);
    v12.Append(va12);
    v13.Append(va13);
    v14.Append(va14);
    v15.Append(va15);
    v16.Append(va16);
    // v17 := ddb.CreateVector(va17);
    v18.Append(va18);
  }
  cols := [] ddb.Vector {v1,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v18};
  colnames := [] string {"tbool","tshort","tint","tlong","date","month","time","minute","second","datetime","timestamp","nanotime","nanotimestamp","tfloat","tdouble","tstring"};
  return ddb.CreateTable(colnames, cols);
}

func main() {
  loopTimes :=10;
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect(host,port,user,pass);
  script :="t = table(100:0, `tbool`tshort`tint`tlong`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`tfloat`tdouble`tstring, [BOOL,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,STRING]); share t as tglobal;";
  script +="login(`admin, `123456); dbPath='dfs://testGo'; if(existsDatabase(dbPath))\ndropDatabase(dbPath); db=database(dbPath, VALUE, 1..5); tb=db.createPartitionedTable(t, `tb, `tint)" 
  conn.Run(script);
 
  var ta ddb.Table;
  var tb ddb.Constant;
  var args []ddb.Constant;
  t := time.Now()
  ta = CreateDemoTable();
  elapsed := time.Since(t)
  fmt.Println("Data generation cost", elapsed)
  t = time.Now()
  for i :=0; i<loopTimes; i++{
    tb = ta.ToConstant();
    args = [] ddb.Constant{tb};
	// conn.RunFunc("tableInsert{tglobal}", args); //in memory table
	  conn.RunFunc("tableInsert{loadTable('dfs://testGo', `tb)}", args); //dfs table
  }
  elapsed = time.Since(t)
  fmt.Println("Insert cost", elapsed)
  // result :=conn.Run("select count(*) from tglobal"); //in memory table
  result :=conn.Run("select count(*) from loadTable('dfs://testGo', `tb)"); //dfs table
  fmt.Println(result.GetString());
  content :=conn.Run("select top 5 * from loadTable('dfs://testGo', `tb)"); //dfs table
  fmt.Println(content.GetString());
}