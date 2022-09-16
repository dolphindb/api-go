package api

import (
	"fmt"
	"strings"

	uuid "github.com/satori/go.uuid"
)

func generateDBName() string {
	return fmt.Sprintf("db_%s", uuid.NewV4().String()[:8])
}

func generateTableName() string {
	return fmt.Sprintf("tb_%s", uuid.NewV4().String()[:8])
}

func generateCreateDatabaseParam(d *DatabaseRequest) string {
	buf := strings.Builder{}

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
