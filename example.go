package main
import (
   "./src"

)
import "fmt"

const(
	host = "127.0.0.1";
	port = 8848;
	username = "admin";
	password = "123456";
)

func main() {
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect(host,port,username,password); 
  v := conn.Run("`IBM`GOOG`YHOO");
  fmt.Println(v.GetString());
}
