package script

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dolphindb/api-go/api"
	"github.com/dolphindb/api-go/example/util"
	"github.com/dolphindb/api-go/model"
)

// CheckDataForm checks whether the DataForm serialization and deserialization are valid.
func CheckDataForm(db api.DolphinDB) {
	// test parse dataForm chart
	by := bytes.NewBuffer([]byte("dates=(2012.01.01..2016.07.31)[def(x):weekday(x) between 1:5]\n"))
	by.WriteString("chartData=each(cumsum,reshape(rand(10000,dates.size()*5)-4500, dates.size():5))\n")
	by.WriteString("chartData.rename!(dates, \"Strategy#\"+string(1..5))\n")
	by.WriteString("plot(chartData,,[\"Cumulative Pnls of Five Strategies\",\"date\",\"pnl\"],LINE)")
	ch, err := db.RunScript(by.String())
	util.AssertNil(err)
	util.AssertEqual(ch.GetDataForm(), model.DfChart)

	// test render datatform vector
	dt, err := model.NewDataType(model.DtString, "key")
	util.AssertNil(err)

	vc := model.NewVector(model.NewDataTypeList(model.DtString, []model.DataType{dt}))
	_, err = db.Upload(map[string]model.DataForm{"vector": vc})
	util.AssertNil(err)

	// test parse datatform vector
	res, err := db.RunScript("vector")
	util.AssertNil(err)
	util.AssertEqual(res.String(), vc.String())
	util.AssertEqual(res.GetDataForm(), model.DfVector)

	// test render datatform vector with arrayvector
	dls, err := model.NewDataTypeListFromRawData(model.DtInt, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9})
	util.AssertNil(err)

	vct := model.NewVector(dls)
	av := model.NewArrayVector([]*model.Vector{vct})

	avc := model.NewVectorWithArrayVector(av)
	_, err = db.Upload(map[string]model.DataForm{"arrvec": avc})
	util.AssertNil(err)

	// test parse datatform vector with arrayvector
	df, err := db.RunScript("arrvec")
	util.AssertNil(err)
	util.AssertEqual(df.String(), avc.String())

	// test render datatform scalar
	s := model.NewScalar(dt)

	_, err = db.Upload(map[string]model.DataForm{"scalar": s})
	util.AssertNil(err)

	// test parse datatform scalar
	res, err = db.RunScript("scalar")
	util.AssertNil(err)
	util.AssertEqual(res.String(), s.String())
	util.AssertEqual(res.GetDataForm(), model.DfScalar)

	// test render datatform set
	set := model.NewSet(vc)
	_, err = db.Upload(map[string]model.DataForm{"set": set})
	util.AssertNil(err)

	// test parse datatform set
	res, err = db.RunScript("set")
	util.AssertNil(err)
	util.AssertEqual(res.String(), set.String())
	util.AssertEqual(res.GetDataForm(), model.DfSet)

	// test render datatform table
	tb := model.NewTable([]string{"key"}, []*model.Vector{vc})
	_, err = db.Upload(map[string]model.DataForm{"table": tb})
	util.AssertNil(err)

	// test parse datatform table
	res, err = db.RunScript("table")
	util.AssertNil(err)
	util.AssertEqual(res.String(), tb.String())
	util.AssertEqual(res.GetDataForm(), model.DfTable)

	dt1, err := model.NewDataType(model.DtString, "value")
	util.AssertNil(err)

	// test render datatform dictionary
	dict := model.NewDictionary(vc, vc)
	_, err = db.Upload(map[string]model.DataForm{"dict": dict})
	util.AssertNil(err)

	// test parse datatform dictionary
	res, err = db.RunScript("dict")
	util.AssertNil(err)
	util.AssertEqual(res.GetDataForm(), model.DfDictionary)

	util.AssertEqual(res.String(), dict.String())
	util.AssertEqual(res.GetDataForm(), model.DfDictionary)

	// test render datatform pair
	vc = model.NewVector(model.NewDataTypeList(model.DtString, []model.DataType{dt, dt1}))
	pair := model.NewPair(vc)

	_, err = db.Upload(map[string]model.DataForm{"pair": pair})
	util.AssertNil(err)

	// test parse datatform pair
	res, err = db.RunScript("pair")
	util.AssertNil(err)
	util.AssertEqual(res.String(), pair.String())
	util.AssertEqual(res.GetDataForm(), model.DfPair)

	// test parse datatform matrix
	mtr, err := db.RunScript("cross(+, 1..5, 1..5)")
	util.AssertNil(err)
	util.AssertEqual(mtr.GetDataForm(), model.DfMatrix)

	// test render datatform matrix
	_, err = db.Upload(map[string]model.DataForm{"mtr": mtr})
	util.AssertNil(err)

	res, err = db.RunScript("mtr")
	util.AssertNil(err)
	util.AssertEqual(res.String(), mtr.String())
	util.AssertEqual(res.GetDataForm(), model.DfMatrix)

	fmt.Println("CheckDataForm Successful")
}

// CheckDataType checks whether the DataType serialization and deserialization are valid.
func CheckDataType(db api.DolphinDB) {
	t := time.Date(1970, time.Month(1), 1, 0, 0, 0, 0, time.UTC).Add(1000 * time.Hour)

	// test render datatype bool
	dt, err := model.NewDataType(model.DtBool, byte(1))
	util.AssertNil(err)

	s := model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"bool": s})
	util.AssertNil(err)

	// test parse datatype bool
	raw, err := db.RunScript("bool")
	util.AssertNil(err)

	res := raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtBool)
	util.AssertEqual(res.String(), s.String())

	// test render datatype bool
	dal, err := model.NewDataTypeListFromRawData(model.DtAny, []model.DataForm{s})
	util.AssertNil(err)

	vc := model.NewVector(dal)
	_, err = db.Upload(map[string]model.DataForm{"any": vc})
	util.AssertNil(err)

	// test parse datatype any
	raw, err = db.RunScript("any")
	util.AssertNil(err)

	v := raw.(*model.Vector)
	util.AssertEqual(v.GetDataForm(), model.DfVector)
	util.AssertEqual(v.String(), vc.String())

	// test render datatype string
	dt, err = model.NewDataType(model.DtString, "example")
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"string": s})
	util.AssertNil(err)

	// test parse datatype string
	raw, err = db.RunScript("string")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtString)
	util.AssertEqual(res.String(), s.String())

	// test render datatype char
	dt, err = model.NewDataType(model.DtChar, byte(97))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"char": s})
	util.AssertNil(err)

	// test parse datatype char
	raw, err = db.RunScript("char")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtChar)
	util.AssertEqual(res.String(), s.String())

	// test render datatype complex
	dt, err = model.NewDataType(model.DtComplex, [2]float64{1, 1})
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"complex": s})
	util.AssertNil(err)

	// test parse datatype complex
	raw, err = db.RunScript("complex")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtComplex)
	util.AssertEqual(res.String(), s.String())

	// test render datatype short
	dt, err = model.NewDataType(model.DtShort, int16(10))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"short": s})
	util.AssertNil(err)

	// test parse datatype short
	raw, err = db.RunScript("short")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtShort)
	util.AssertEqual(res.String(), s.String())

	// test render datatype blob
	dt, err = model.NewDataType(model.DtBlob, []byte{10, 12, 14, 56})
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"blob": s})
	util.AssertNil(err)

	// test parse datatype blob
	raw, err = db.RunScript("blob")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtBlob)
	util.AssertEqual(res.String(), s.String())

	// test render datatype date
	dt, err = model.NewDataType(model.DtDate, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"date": s})
	util.AssertNil(err)

	// test parse datatype date
	raw, err = db.RunScript("date")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtDate)
	util.AssertEqual(res.String(), s.String())

	// test render datatype datehour
	dt, err = model.NewDataType(model.DtDateHour, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"datehour": s})
	util.AssertNil(err)

	// test parse datatype datehour
	raw, err = db.RunScript("datehour")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtDateHour)
	util.AssertEqual(res.String(), s.String())

	// test render datatype datetime
	dt, err = model.NewDataType(model.DtDatetime, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"datetime": s})
	util.AssertNil(err)

	// test parse datatype datetime
	raw, err = db.RunScript("datetime")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtDatetime)
	util.AssertEqual(res.String(), s.String())

	// test render datatype double
	dt, err = model.NewDataType(model.DtDouble, float64(1))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"double": s})
	util.AssertNil(err)

	// test parse datatype double
	raw, err = db.RunScript("double")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtDouble)
	util.AssertEqual(res.String(), s.String())

	// test render datatype float
	dt, err = model.NewDataType(model.DtFloat, float32(1.0))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"float": s})
	util.AssertNil(err)

	// test parse datatype float
	raw, err = db.RunScript("float")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtFloat)
	util.AssertEqual(res.String(), s.String())

	// test render datatype duration
	dt, err = model.NewDataType(model.DtDuration, "10H")
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"duration": s})
	util.AssertNil(err)

	// test parse datatype duration
	raw, err = db.RunScript("duration")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtDuration)
	util.AssertEqual(res.String(), s.String())

	// test render datatype int
	dt, err = model.NewDataType(model.DtInt, int32(10))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"int": s})
	util.AssertNil(err)

	// test parse datatype int
	raw, err = db.RunScript("int")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtInt)
	util.AssertEqual(res.String(), s.String())

	// test render datatype int128
	dt, err = model.NewDataType(model.DtInt128, "e1671797c52e15f763380b45e841ec32")
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"int128": s})
	util.AssertNil(err)

	// test parse datatype int128
	raw, err = db.RunScript("int128")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtInt128)
	util.AssertEqual(res.String(), s.String())

	// test render datatype ip
	dt, err = model.NewDataType(model.DtIP, "346b:6c2a:3347:d244:7654:5d5a:bcbb:5dc7")
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"ip": s})
	util.AssertNil(err)

	// test parse datatype ip
	raw, err = db.RunScript("ip")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtIP)
	util.AssertEqual(res.String(), s.String())

	// test render datatype long
	dt, err = model.NewDataType(model.DtLong, int64(100))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"long": s})
	util.AssertNil(err)

	// test parse datatype long
	raw, err = db.RunScript("long")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtLong)
	util.AssertEqual(res.String(), s.String())

	// test render datatype minute
	dt, err = model.NewDataType(model.DtMinute, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"minute": s})
	util.AssertNil(err)

	// test parse datatype minute
	raw, err = db.RunScript("minute")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtMinute)
	util.AssertEqual(res.String(), s.String())

	// test render datatype month
	dt, err = model.NewDataType(model.DtMonth, time.Date(2021, 5, 1, 1, 1, 1, 1, time.UTC))
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"month": s})
	util.AssertNil(err)

	// test parse datatype month
	raw, err = db.RunScript("month")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtMonth)
	util.AssertEqual(res.String(), s.String())

	// test render datatype nanotime
	dt, err = model.NewDataType(model.DtNanoTime, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"nanotime": s})
	util.AssertNil(err)

	// test parse datatype nanotime
	raw, err = db.RunScript("nanotime")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtNanoTime)
	util.AssertEqual(res.String(), s.String())

	// test render datatype nanotimestamp
	dt, err = model.NewDataType(model.DtNanoTimestamp, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"nanotimestamp": s})
	util.AssertNil(err)

	// test parse datatype nanotimestamp
	raw, err = db.RunScript("nanotimestamp")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtNanoTimestamp)
	util.AssertEqual(res.String(), s.String())

	// test render datatype point
	dt, err = model.NewDataType(model.DtPoint, [2]float64{10, 10})
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"point": s})
	util.AssertNil(err)

	// test parse datatype point
	raw, err = db.RunScript("point")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtPoint)
	util.AssertEqual(res.String(), s.String())

	// test render datatype second
	dt, err = model.NewDataType(model.DtSecond, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"second": s})
	util.AssertNil(err)

	// test parse datatype second
	raw, err = db.RunScript("second")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtSecond)
	util.AssertEqual(res.String(), s.String())

	// test render datatype time
	dt, err = model.NewDataType(model.DtTime, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"time": s})
	util.AssertNil(err)

	// test parse datatype time
	raw, err = db.RunScript("time")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtTime)
	util.AssertEqual(res.String(), s.String())

	// test render datatype timestamp
	dt, err = model.NewDataType(model.DtTimestamp, t)
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"timestamp": s})
	util.AssertNil(err)

	// test parse datatype timestamp
	raw, err = db.RunScript("timestamp")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtTimestamp)
	util.AssertEqual(res.String(), s.String())

	// test render datatype uuid
	dt, err = model.NewDataType(model.DtUUID, "e5eca940-5b99-45d0-bf1c-620f6b1b9d5b")
	util.AssertNil(err)

	s = model.NewScalar(dt)
	_, err = db.Upload(map[string]model.DataForm{"uuid": s})
	util.AssertNil(err)

	// test parse datatype uuid
	raw, err = db.RunScript("uuid")
	util.AssertNil(err)

	res = raw.(*model.Scalar)
	util.AssertEqual(res.GetDataForm(), model.DfScalar)
	util.AssertEqual(res.DataType.DataType(), model.DtUUID)
	util.AssertEqual(res.String(), s.String())

	fmt.Println("CheckDataType Successful")
}
