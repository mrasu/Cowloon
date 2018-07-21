package db

import (
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/schema"
)

type Query struct {
	Sql  string
	Args []interface{}
}

func NewQuery(sql string, args []interface{}) *Query {
	return &Query{
		Sql:  sql,
		Args: args,
	}
}

func BuildInsertQuery(tableName string, columns []schema.TableColumn, args []interface{}) *Query {
	var cNames []string
	var ph []string
	for _, column := range columns {
		cNames = append(cNames, column.Name)
		ph = append(ph, "?")
	}

	sql := fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s)", tableName, strings.Join(cNames, ", "), strings.Join(ph, ", "))
	return NewQuery(sql, args)
}
