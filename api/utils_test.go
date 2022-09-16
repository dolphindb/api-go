package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUtils(t *testing.T) {
	assert.NotEqual(t, generateTableName(), generateTableName())
	assert.NotEqual(t, generateDBName(), generateDBName())

	dReq := &DatabaseRequest{
		DBHandle:        "db",
		Directory:       "dfs://db",
		PartitionType:   "t",
		PartitionScheme: "s",
		Locations:       "l",
		Engine:          "e",
		Atomic:          "a",
	}

	assert.Equal(t, generateCreateDatabaseParam(dReq), "directory='dfs://db',partitionType=t,partitionScheme=s,locations=l,Engine='e',Atomic='a'")
}
