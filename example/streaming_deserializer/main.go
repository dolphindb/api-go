package main

import (
	"context"

	"github.com/dolphindb/api-go/v3/api"
	"github.com/dolphindb/api-go/v3/example/util"
	"github.com/dolphindb/api-go/v3/logging"
	"github.com/dolphindb/api-go/v3/streaming"
)

const partitionedReplayScript = `
try{dropStreamTable(` + "`outTables" + `)}catch(ex){}
share streamTable(100:0, ` + "`timestampv`sym`blob`price1" + `,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables

n = 6;
dbName = 'dfs://test_StreamDeserializer_pair'
if(existsDatabase(dbName)){
    dropDB(dbName)}
db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
table1 = table(100:0, ` + "`datetimev`timestampv`sym`price1`price2" + `, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
table2 = table(100:0, ` + "`datetimev`timestampv`sym`price1" + `, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(` + "`a`b`c" + `,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(` + "`a`b`c" + `,n), rand(100,n)+rand(1.0, n))
pt1 = db.createPartitionedTable(table1,'pt1',` + "`datetimev" + `).append!(table1)
pt2 = db.createPartitionedTable(table2,'pt2',` + "`datetimev" + `).append!(table2)

re1 = replayDS(sqlObj=<select * from pt1>, dateColumn=` + "`datetimev" + `, timeColumn=` + "`timestampv" + `)
re2 = replayDS(sqlObj=<select * from pt2>, dateColumn=` + "`datetimev" + `, timeColumn=` + "`timestampv" + `)
d = dict(['msg1', 'msg2'], [re1, re2])
replay(inputTables=d, outputTables=` + "`outTables" + `, dateColumn=` + "`timestampv" + `, timeColumn=` + "`timestampv" + `)
`

const memoryReplayScript = `
try{dropStreamTable(` + "`outTables" + `)}catch(ex){}
share streamTable(100:0, ` + "`timestampv`sym`blob`price1" + `,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables

n = 6;
table1 = table(100:0, ` + "`datetimev`timestampv`sym`price1`price2" + `, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
table2 = table(100:0, ` + "`datetimev`timestampv`sym`price1" + `, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(` + "`a`b`c" + `,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(` + "`a`b`c" + `,n), rand(100,n)+rand(1.0, n))
share table1 as pt1
share table2 as pt2

d = dict(['msg1', 'msg2'], [pt1, pt2])
replay(inputTables=d, outputTables=` + "`outTables" + `, dateColumn=` + "`timestampv" + `, timeColumn=` + "`timestampv" + `)
`

type sampleHandler struct {
	sd streaming.StreamDeserializer
}

func (s *sampleHandler) DoEvent(msg streaming.IMessage) {
	ret, err := s.sd.Parse(msg)
	if err != nil {
		logging.Error("example.streaming_deserializer", "parse failed", "err", err)
		return
	}

	values := make([]string, 0, ret.Size())
	for i := 0; i < ret.Size(); i++ {
		values = append(values, ret.GetValue(i).String())
	}
	logging.Info("example.streaming_deserializer", "parsed message", "symbol", ret.GetSym(), "values", values)
}

func newPartitionedDeserializer(conn api.DolphinDB) (*streaming.StreamDeserializer, error) {
	tableNames := map[string][2]string{
		"msg1": {"dfs://test_StreamDeserializer_pair", "pt1"},
		"msg2": {"dfs://test_StreamDeserializer_pair", "pt2"},
	}

	return streaming.NewStreamDeserializer(&streaming.StreamDeserializerOption{
		TableNames: tableNames,
		Conn:       conn,
	})
}

func newMemoryDeserializer(conn api.DolphinDB) (*streaming.StreamDeserializer, error) {
	tableNames := map[string][2]string{
		"msg1": {"", "pt1"},
		"msg2": {"", "pt2"},
	}

	return streaming.NewStreamDeserializer(&streaming.StreamDeserializerOption{
		TableNames: tableNames,
		Conn:       conn,
	})
}

func main() {
	util.InitExampleLogger()

	_ = context.Background()
	logging.Info("example.streaming_deserializer", "partitioned replay setup script", "script", partitionedReplayScript)
	logging.Info("example.streaming_deserializer", "memory replay setup script", "script", memoryReplayScript)
	logging.Info("example.streaming_deserializer", "use newPartitionedDeserializer/newMemoryDeserializer with a connected client before subscribing")
}
