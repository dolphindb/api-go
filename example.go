package main
import (
	 "./api"
)
func main() {
  var conn ddb.DBConnection;
  conn.Init();
  conn.Connect("localhost",1321,"admin","123456");
  
}