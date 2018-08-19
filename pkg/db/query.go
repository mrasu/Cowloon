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

func BuildInsertQuery(tableName string, columns []schema.TableColumn, args []interface{}) (*Query, error) {
	if len(columns) != len(args) {
		return nil, fmt.Errorf("clauses for INSERT don't have same length, (columns: %d, args: %d)", len(columns), len(args))
	}

	var cNames []string
	var ph []string
	for _, column := range columns {
		cNames = append(cNames, column.Name)
		ph = append(ph, "?")
	}

	sql := fmt.Sprintf("REPLACE INTO %s (%s) VALUES (%s)", tableName, strings.Join(cNames, ", "), strings.Join(ph, ", "))
	return NewQuery(sql, args), nil
}

func BuildUpdateQuery(tableName string, columns []schema.TableColumn, whereValues []interface{}, setValues []interface{}) (*Query, error) {
	if len(columns) != len(whereValues) || len(columns) != len(setValues) {
		return nil, fmt.Errorf("clauses for UPDATE don't have same length, (columns: %d, where: %d, update: %d)", len(columns), len(whereValues), len(setValues))
	}
	whereClause := buildPreparedWhereClause(columns)
	setClause := buildPreparedSetClause(columns)

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s ", tableName, setClause, whereClause)

	var args []interface{}
	for _, val := range setValues {
		args = append(args, val)
	}
	for _, val := range whereValues {
		args = append(args, val)
	}

	return NewQuery(sql, args), nil
}

func buildPreparedWhereClause(columns []schema.TableColumn) string {
	var whereClauses []string
	for _, column := range columns {
		whereClauses = append(whereClauses, column.Name+"=?")
	}
	return strings.Join(whereClauses, " AND ")
}

func buildPreparedSetClause(columns []schema.TableColumn) string {
	var whereClauses []string
	for _, column := range columns {
		whereClauses = append(whereClauses, column.Name+"=?")
	}
	return strings.Join(whereClauses, ",")
}

func BuildDeleteQuery(tableName string, columns []schema.TableColumn, args []interface{}) (*Query, error) {
	if len(columns) != len(args) {
		return nil, fmt.Errorf("clauses for DELETE don't have same length, (columns: %d, where: %d)", len(columns), len(args))
	}

	whereClause := buildPreparedWhereClause(columns)

	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, whereClause)
	return NewQuery(sql, args), nil
}
