package api

import (
	"fmt"

	"github.com/dolphindb/api-go/dialer"
	"github.com/dolphindb/api-go/model"
)

// TableAppender is used to append tables into another.
type TableAppender struct {
	// DBPath of database
	DBPath string
	// Name of table
	TableName string

	// conn which has connected to and logged in the dolphindb server
	Conn dialer.Conn

	columnTypes []model.DataTypeByte
	nameList    []string
}

// TableAppenderOption helps you to init TableAppender.
type TableAppenderOption struct {
	// DBPath of table
	DBPath string
	// Name of table
	TableName string
	// Conn which has connected to and logged in the dolphindb server
	Conn dialer.Conn
}

// NewTableAppender instantiates a new TableAppender object according to the option.
func NewTableAppender(opt *TableAppenderOption) *TableAppender {
	ta := &TableAppender{
		Conn:      opt.Conn,
		DBPath:    opt.DBPath,
		TableName: opt.TableName,
	}

	var script string
	if opt.DBPath == "" {
		script = fmt.Sprintf("schema(%s)", opt.TableName)
	} else {
		script = fmt.Sprintf("schema(loadTable(\"%s\", \"%s\"))", opt.DBPath, opt.TableName)
	}

	ret, err := opt.Conn.RunScript(script)
	if err != nil {
		fmt.Printf("Failed to get table %s schema: %s\n", opt.TableName, err.Error())
		return nil
	}

	err = packTableAppenderWithColDefs(ret, ta)
	if err != nil {
		fmt.Printf("Failed to get colDefs from table: %s\n", err.Error())
		return nil
	}

	return ta
}

func packTableAppenderWithColDefs(ret model.DataForm, ta *TableAppender) error {
	tableInfo := ret.(*model.Dictionary)
	dt, err := tableInfo.Get("colDefs")
	if err != nil {
		fmt.Printf("Failed to get colDefs from table: %s\n", err.Error())
		return err
	}

	schema := dt.Value().(*model.Table)

	typeList := schema.GetColumnByName("typeInt")
	ta.columnTypes = make([]model.DataTypeByte, typeList.Data.Len())
	for i := 0; i < typeList.Data.Len(); i++ {
		raw := typeList.Data.ElementValue(i)
		ta.columnTypes[i] = model.DataTypeByte(raw.(int32))
	}

	ta.nameList = schema.GetColumnByName("name").Data.StringList()

	return nil
}

// Close closes the connection.
func (p *TableAppender) Close() error {
	return p.Conn.Close()
}

// IsClosed checks whether the TableAppender is closed.
func (p *TableAppender) IsClosed() bool {
	return p.Conn.IsClosed()
}

// Append appends a table to the table which has been set when calling NewTableAppender.
func (p *TableAppender) Append(tb *model.Table) (model.DataForm, error) {
	paramTable, err := p.packageTable(tb)
	if err != nil {
		fmt.Printf("Failed to package table: %s\n", err.Error())
		return nil, err
	}

	if p.DBPath == "" {
		return p.Conn.RunFunc(fmt.Sprintf("append!{%s}", p.TableName), []model.DataForm{paramTable})
	}

	return p.Conn.RunFunc(fmt.Sprintf("append!{loadTable(\"%s\",\"%s\"), }",
		p.DBPath, p.TableName), []model.DataForm{paramTable})
}

func (p *TableAppender) packageTable(tb *model.Table) (*model.Table, error) {
	cols := make([]*model.Vector, len(p.nameList))
	for k := range p.nameList {
		srcVct := tb.GetColumnByIndex(k)
		srcDt := srcVct.GetDataType()
		if (srcDt == model.DtDate || srcDt == model.DtMonth || srcDt == model.DtTime || srcDt == model.DtMinute ||
			srcDt == model.DtSecond || srcDt == model.DtDatetime || srcDt == model.DtTimestamp || srcDt == model.DtNanoTime ||
			srcDt == model.DtNanoTimestamp || srcDt == model.DtDateHour) && srcDt != p.columnTypes[k] {
			raw, err := model.CastDateTime(tb.GetColumnByIndex(k), p.columnTypes[k])
			if err != nil {
				fmt.Printf("Failed to cast DateTime before appending: %s\n", err.Error())
				return nil, err
			}

			cols[k] = raw.(*model.Vector)
		} else {
			cols[k] = srcVct
		}
	}

	return model.NewTable(p.nameList, cols), nil
}
