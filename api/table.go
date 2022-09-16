package api

import (
	"github.com/dolphindb/api-go/model"
)

// Table is the client of table script.
// TODO: supports table script.
type Table struct {
	db *dolphindb

	// selectSQL []string
	// whereSQL  []string
	// script    string
	// isExec    bool
	// schemaInit bool

	// Handle is the handle of Table which has been defined on server
	Handle string
	// Data is the real value of Table
	Data *model.Table
}

// func (t *Table) setDB(db *dolphindb) *Table {
// 	t.db = db
// 	return t
// }

// func (t *Table) setData(data *model.Table) *Table {
// 	t.Data = data
// 	return t
// }

// func (t *Table) setHandle(name string) *Table {
// 	t.Handle = name
// 	return t
// }

// GetHandle returns the variable name of Table which has been defined on server.
func (t *Table) GetHandle() string {
	return t.Handle
}

// GetSession returns the session id of the connection to dolphindb.
func (t *Table) GetSession() string {
	return t.db.GetSession()
}

// String returns the string format of the data.
func (t *Table) String() string {
	return t.Data.String()
}

// func (t *Table) Select(s []string) *Table {
// 	t.selectSql = s
// 	t.schemaInit = true
// 	return t
// }

// func (t *Table) Exec(s []string) *Table {
// 	t.selectSql = s
// 	t.isExec = true
// 	t.schemaInit = true
// 	return t
// }

// func (t *Table) Where(w []string) *Table {
// 	if len(w) == 0 {
// 		return t
// 	}

// 	if t.whereSql == nil {
// 		t.whereSql = make([]string, 0)
// 	}

// 	t.whereSql = append(t.whereSql, w...)
// 	return t
// }

// func (t *Table) initSchema() error {
// 	raw, err := t.db.RunScript(fmt.Sprintf("schema(%s)", t.Handle))
// 	if err != nil {
// 		return err
// 	}

// 	dict := raw.(*model.Dictionary)
// 	keys, err := dict.Keys.Data.StringList()
// 	if err != nil {
// 		return err
// 	}

// 	for k, v := range keys {
// 		if v == "colDefs" {
// 			dt := dict.Values.Data.Get(k)
// 			ta := dt.DataForm().(*model.Table)
// 			for k, v := range ta.Columns {
// 				str, err := k.String()
// 				if err != nil {
// 					return err
// 				}

// 				if str == "name" {
// 					names, err := v.Data.StringList()
// 					if err != nil {
// 						return err
// 					}
// 					t.Select(names)
// 					return nil
// 				}
// 			}
// 		}
// 	}

// 	return errors.New("init schema failed: there is no column in table")
// }

// func (t *Table) ToDF() (model.DataForm, error) {
// 	if !t.schemaInit {
// 		err := t.initSchema()
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
// 	sql := t.Sql()
// 	df, err := t.db.RunScript(sql)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return df, nil
// }

// func (t *Table) Sql() string {
// 	return ""
// }
