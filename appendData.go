package main

import (
   "./api"
   "fmt"
   "time"
)

const(
	host = "localhost";
	port = 8921;
	user = "admin";
	pass = "123456";
)

func CreateDemoTable() ddb.Table{
  vecLen := 0;
  v1 := ddb.CreateVector(ddb.DT_BOOL, vecLen);
  // v2 := ddb.CreateVector(ddb.DT_CAHR, vecLen);
  v3 := ddb.CreateVector(ddb.DT_SHORT, vecLen);
  v4 := ddb.CreateVector(ddb.DT_INT, vecLen);
  v5 := ddb.CreateVector(ddb.DT_LONG, vecLen);
  v6 := ddb.CreateVector(ddb.DT_DATE, vecLen);
  v7 := ddb.CreateVector(ddb.DT_MONTH, vecLen);
  v8 := ddb.CreateVector(ddb.DT_TIME, vecLen);
  v9 := ddb.CreateVector(ddb.DT_MINUTE, vecLen);
  v10 := ddb.CreateVector(ddb.DT_SECOND, vecLen);
  v11 := ddb.CreateVector(ddb.DT_DATETIME, vecLen);
  v12 := ddb.CreateVector(ddb.DT_TIMESTAMP, vecLen);
  v13 := ddb.CreateVector(ddb.DT_NANOTIME, vecLen);
  v14 := ddb.CreateVector(ddb.DT_NANOTIMESTAMP, vecLen);
  v15 := ddb.CreateVector(ddb.DT_FLOAT, vecLen);
  v16 := ddb.CreateVector(ddb.DT_DOUBLE, vecLen);
  // v17 := ddb.CreateVector(ddb.DT_SYMBOL, vecLen);
  v18 := ddb.CreateVector(ddb.DT_STRING, vecLen);

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
  
  rowNum := 100000;
  for i :=0; i<rowNum; i++{
    v1.Append(va1);
    // v2.Append(va2);
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
    // v17.Append(va17);
    v18.Append(va18);
  }
  cols := [] ddb.Vector {v1,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v18};
  colnames := [] string {"tbool","tshort","tint","tlong","date","month","time","minute","second","datetime","timestamp","nanotime","nanotimestamp","tfloat","tdouble","tstring"};
  return ddb.CreateTableByVector(colnames, cols);
}

func CreateDemoTableFast() ddb.Table{   
  rowNum := 100000;

  v1 := ddb.CreateVector(ddb.DT_BOOL,rowNum);
  v2 := ddb.CreateVector(ddb.DT_SHORT, rowNum);
  v3 := ddb.CreateVector(ddb.DT_INT, rowNum);
  v4 := ddb.CreateVector(ddb.DT_LONG, rowNum);
  v5 := ddb.CreateVector(ddb.DT_FLOAT, rowNum);
  v6 := ddb.CreateVector(ddb.DT_DOUBLE, rowNum);
  v7 := ddb.CreateVector(ddb.DT_STRING, rowNum);

  var arr1 []bool;
  var arr2 []int16;
  var arr3 []int32;
  var arr4 []int64;
  var arr5 []float32;
  var arr6 []float64;
  var arr7 []string;
  
  arrSize := 100000;
  for i := 0; i<arrSize; i++{
    arr1 = append(arr1, true);
    arr2 = append(arr2, 1);
    arr3 = append(arr3, 1);
    arr4 = append(arr4, 1);
    arr5 = append(arr5, 1.0);
    arr6 = append(arr6, 1.0);
    arr7 = append(arr7, "1");
  }

  start := 0;
  v1.SetBoolArray(start,arrSize,arr1);
  v2.SetShortArray(start,arrSize,arr2);
  v3.SetIntArray(start,arrSize,arr3);  
  v4.SetLongArray(start,arrSize,arr4);
  v5.SetFloatArray(start,arrSize,arr5);  
  v6.SetDoubleArray(start,arrSize,arr6);
  v7.SetStringArray(start,arrSize,arr7);    

  cols := [] ddb.Vector {v1,v2,v3,v4,v5,v6,v7};
  colnames := [] string {"tbool","tshort","tint","tlong","tfloat","tdouble","tstring"};
  return ddb.CreateTableByVector(colnames, cols);
}

func CreateDemoTableSlow() ddb.Table{
  vecLen := 0;

  v1 := ddb.CreateVector(ddb.DT_BOOL, vecLen);
  v2 := ddb.CreateVector(ddb.DT_SHORT, vecLen);
  v3 := ddb.CreateVector(ddb.DT_INT, vecLen);
  v4 := ddb.CreateVector(ddb.DT_LONG, vecLen);
  v5 := ddb.CreateVector(ddb.DT_FLOAT, vecLen);
  v6 := ddb.CreateVector(ddb.DT_DOUBLE, vecLen);
  v7 := ddb.CreateVector(ddb.DT_STRING, vecLen);

  va1 := ddb.CreateBool(true);
  va2 := ddb.CreateShort(1);
  va3 := ddb.CreateInt(1);
  va4 := ddb.CreateLong(1);
  va5 := ddb.CreateFloat(1.0);
  va6 := ddb.CreateDouble(1.0);
  va7 := ddb.CreateString("1");

  rowNum :=100000;
  for i := 0; i<rowNum; i++{
    v1.Append(va1);
    v2.Append(va2);
    v3.Append(va3);
    v4.Append(va4);
    v5.Append(va5);
    v6.Append(va6);
    v7.Append(va7);
  }
  cols := [] ddb.Vector {v1,v2,v3,v4,v5,v6,v7};
  colnames := [] string {"tbool","tshort","tint","tlong","tfloat","tdouble","tstring"};
  return ddb.CreateTableByVector(colnames, cols);
}

func assertEqual(a ddb.Constant, b ddb.Constant) bool{
	sa:=a.GetString();
	sb:=b.GetString();
	if sa == sb {
		return true;
	} else {
		return false;
	}
}

func TestUpdateFuncs(){
	rowNum := 6;
  
	v1 := ddb.CreateVector(ddb.DT_BOOL,rowNum);
	v2 := ddb.CreateVector(ddb.DT_SHORT, rowNum);
	v3 := ddb.CreateVector(ddb.DT_INT, rowNum);
	v4 := ddb.CreateVector(ddb.DT_LONG, rowNum);
	v5 := ddb.CreateVector(ddb.DT_FLOAT, rowNum);
	v6 := ddb.CreateVector(ddb.DT_DOUBLE, rowNum);
	v7 := ddb.CreateVector(ddb.DT_STRING, rowNum);
  
	arr1 := [] bool {true,false,true,false,true,false};
	arr2 := [] int16 {11,14,13,12,15,10};
	arr3 := [] int32 {11,14,13,12,15,10};
	arr4 := [] int64 {33000,78546,54796,63215,84132,52369};
	arr5 := [] float32 {1.6,7.0,2.8,1.7,10.4,4.8};
	arr6 := [] float64 {101.6,97.2,12.88,51.7,10.4,74.8};
	arr7 := [] string {"q","a","d","w","q","r"};
  
	start := 0;
	v1.SetBoolArray(start,rowNum,arr1);
	v2.SetShortArray(start,rowNum,arr2);
	v3.SetIntArray(start,rowNum,arr3);  
	v4.SetLongArray(start,rowNum,arr4);
	v5.SetFloatArray(start,rowNum,arr5);  
	v6.SetDoubleArray(start,rowNum,arr6);
	v7.SetStringArray(start,rowNum,arr7);    
  
	v1.SetBoolByIndex(4,false);
	v2.SetShortByIndex(4,5);
	v3.SetIntByIndex(4,7);
	v4.SetLongByIndex(4,19843);
	v5.SetFloatByIndex(4,8.2);
	v6.SetDoubleByIndex(4,4.6);
	v7.SetStringByIndex(4,"ii");

	cv1 := ddb.CreateVector(ddb.DT_BOOL,rowNum);
	cv2 := ddb.CreateVector(ddb.DT_SHORT, rowNum);
	cv3 := ddb.CreateVector(ddb.DT_INT, rowNum);
	cv4 := ddb.CreateVector(ddb.DT_LONG, rowNum);
	cv5 := ddb.CreateVector(ddb.DT_FLOAT, rowNum);
	cv6 := ddb.CreateVector(ddb.DT_DOUBLE, rowNum);
	cv7 := ddb.CreateVector(ddb.DT_STRING, rowNum);

  carr1 := [] bool {true,false,true,false,false,false};
	carr2 := [] int16 {11,14,13,12,5,10};
	carr3 := [] int32 {11,14,13,12,7,10};
	carr4 := [] int64 {33000,78546,54796,63215,19843,52369};
	carr5 := [] float32 {1.6,7.0,2.8,1.7,8.2,4.8};
	carr6 := [] float64 {101.6,97.2,12.88,51.7,4.6,74.8};
	carr7 := [] string {"q","a","d","w","ii","r"};

	cv1.SetBoolArray(start,rowNum,carr1);
	cv2.SetShortArray(start,rowNum,carr2);
	cv3.SetIntArray(start,rowNum,carr3);  
	cv4.SetLongArray(start,rowNum,carr4);
	cv5.SetFloatArray(start,rowNum,carr5);  
	cv6.SetDoubleArray(start,rowNum,carr6);
	cv7.SetStringArray(start,rowNum,carr7);   
 
  ress:=assertEqual(v1.ToConstant(),cv1.ToConstant());
  if ress == false {
		fmt.Println(ress);
	}
  ress=assertEqual(v2.ToConstant(),cv2.ToConstant());
  if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(v3.ToConstant(),cv3.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(v4.ToConstant(),cv4.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(v5.ToConstant(),cv5.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(v6.ToConstant(),cv6.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(v7.ToConstant(),cv7.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}

	va1 := ddb.CreateBool(true);
	va2 := ddb.CreateShort(1);
	va3 := ddb.CreateInt(1);
	va4 := ddb.CreateLong(1);
	va5 := ddb.CreateFloat(1.0);
	va6 := ddb.CreateDouble(1.0);
	va7 := ddb.CreateString("1");
  
	va1.SetBool(false);
	va2.SetShort(3);
	va3.SetInt(3);
	va4.SetLong(35646);
	va5.SetFloat(3.2);
	va6.SetDouble(5.6);
	va7.SetString("rw");
	
	cva1 := ddb.CreateBool(false);
	cva2 := ddb.CreateShort(3);
	cva3 := ddb.CreateInt(3);
	cva4 := ddb.CreateLong(35646);
	cva5 := ddb.CreateFloat(3.2);
	cva6 := ddb.CreateDouble(5.6);
	cva7 := ddb.CreateString("rw");

	ress=assertEqual(va1.ToConstant(),cva1.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(va2.ToConstant(),cva2.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(va3.ToConstant(),cva3.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(va4.ToConstant(),cva4.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(va5.ToConstant(),cva5.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(va6.ToConstant(),cva6.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
	ress=assertEqual(va7.ToConstant(),cva7.ToConstant());
	if ress == false {
		fmt.Println(ress);
	}
}

func main() {
  loopTimes := 10;
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect(host,port,user,pass);
  script := "t = table(100:0, `tbool`tshort`tint`tlong`tfloat`tdouble`tstring, [BOOL,SHORT,INT,LONG,FLOAT,DOUBLE,STRING]); share t as tglobal;";
  script += "login(`admin, `123456); dbPath='dfs://testGo'; if(existsDatabase(dbPath))\ndropDatabase(dbPath); db=database(dbPath, VALUE, 1..5); tb=db.createPartitionedTable(t, `tb, `tint)" 
  conn.Run(script);
 
  var ta ddb.Table;
  var tb ddb.Constant;
  var args []ddb.Constant;

  t := time.Now()
  tab := CreateDemoTableSlow();
  elapsed := time.Since(t);
  fmt.Println("Slow data generation cost", elapsed);
  tb1 := tab.ToConstant();
  conn.Upload("tab", tb1);
  result := conn.Run("select count(*) from tab");
  fmt.Println(result.GetString());

  t = time.Now()
  ta = CreateDemoTableFast();
  elapsed = time.Since(t);
  fmt.Println("Fast data generation cost", elapsed);

  t = time.Now();
  for i := 0; i<loopTimes; i++{
    tb = ta.ToConstant();
    args = [] ddb.Constant{tb};
	  conn.RunFunc("tableInsert{loadTable('dfs://testGo', `tb)}", args);
  }
  elapsed1 := time.Since(t)
  fmt.Println("Insertion cost", elapsed1)

  result = conn.Run("select count(*) from loadTable('dfs://testGo', `tb)");
  fmt.Println(result.GetString());
  content := conn.Run("select top 5 * from loadTable('dfs://testGo', `tb)");
  fmt.Println(content.GetString());

  TestUpdateFuncs();
}
