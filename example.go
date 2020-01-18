package main
import (
   "./api"

)
const(
	host = "localhost";
	port = 8848;
	username = "admin";
	password = "123456";
)

func main() {
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect(host,port,username,password); 

}