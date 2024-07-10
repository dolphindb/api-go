package api

import (
	"fmt"
	"strings"

	uuid "github.com/satori/go.uuid"
)

func generateDBName() string {
	u1 := uuid.NewV4()
	return fmt.Sprintf("db_%s", u1.String()[:8])
}

func generateTableName() string {
	u1 := uuid.NewV4()
	return fmt.Sprintf("tb_%s", u1.String()[:8])
}

func generateCreateDatabaseParam(d *DatabaseRequest) string {
	buf := &strings.Builder{}

	if d.Directory != "" {
		buf.WriteString("directory='")
		buf.WriteString(d.Directory)
		buf.WriteString("',")
	}

	if d.PartitionType != "" {
		buf.WriteString("partitionType=")
		buf.WriteString(d.PartitionType)
		buf.WriteString(",")
	}

	if d.PartitionScheme != "" {
		buf.WriteString("partitionScheme=")
		buf.WriteString(d.PartitionScheme)
		buf.WriteString(",")
	}

	if d.Locations != "" {
		buf.WriteString("locations=")
		buf.WriteString(d.Locations)
		buf.WriteString(",")
	}

	if d.Engine != "" {
		buf.WriteString("Engine='")
		buf.WriteString(d.Engine)
		buf.WriteString("',")
	}

	if d.Atomic != "" {
		buf.WriteString("Atomic='")
		buf.WriteString(d.Atomic)
		buf.WriteString("',")
	}

	return strings.TrimSuffix(buf.String(), ",")
}

func generateCreatePatitionedTable(handle, dbName string, p *CreatePartitionedTableRequest) string {
	buf := &strings.Builder{}

	buf.WriteString(fmt.Sprintf("%s=%s.createPartitionedTable(%s, `%s, `%s", handle, dbName,
		p.SrcTable, p.PartitionedTableName, strings.Join(p.PartitionColumns, "`")))

	if len(p.CompressMethods) > 0 {
		buf.WriteString(",compressMethods={")
		cm := make([]string, 0, len(p.CompressMethods))
		for k, v := range p.CompressMethods {
			cm = append(cm, fmt.Sprintf(`%s:"%s"`, k, v))
		}
		buf.WriteString(strings.Join(cm, ","))
		buf.WriteString("}")
	}

	if len(p.SortColumns) > 0 {
		buf.WriteString(fmt.Sprintf(",sortColumns=`%s", strings.Join(p.SortColumns, "`")))
	}

	if len(p.KeepDuplicates) > 0 {
		buf.WriteString(",keepDuplicates=" + p.KeepDuplicates)
	}
	buf.WriteString(")")
	return buf.String()
}

func generateCreateTable(handle, dbName string, t *CreateTableRequest) string {
	by := &strings.Builder{}
	by.WriteString(handle)
	by.WriteString("=")
	by.WriteString(dbName)
	by.WriteString(".createTable(")
	by.WriteString(t.SrcTable)
	by.WriteString(",`")
	by.WriteString(t.DimensionTableName)

	if len(t.SortColumns) > 0 {
		by.WriteString(",sortColumns=`")
		by.WriteString(strings.Join(t.SortColumns, "`"))
	}

	by.WriteString(")")
	return by.String()
}

func generateSaveTableParam(d *SaveTableRequest) string {
	buf := strings.Builder{}
	buf.WriteString(d.DBHandle)
	buf.WriteString(", ")
	buf.WriteString(d.Table)
	if d.TableName != "" {
		buf.WriteString(", `")
		buf.WriteString(d.TableName)

		buf.WriteString(", ")
		if d.Appending {
			buf.WriteString("1")
		} else {
			buf.WriteString("0")
		}

		buf.WriteString(", ")
		if d.Compression {
			buf.WriteString("1")
		} else {
			buf.WriteString("0")
		}
	}

	return buf.String()
}
