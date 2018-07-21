package db

import (
	"database/sql"

	"fmt"

	"log"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/mrasu/Cowloon/pkg/protos"
	"github.com/pkg/errors"
)

type ShardConnection struct {
	User     string
	Addr     string
	DbName   string
	dataBase *sql.DB
}

func NewShardConnection(user, addr, db string) (*ShardConnection, error) {
	ds := toDataSource(user, addr, db)
	dataBase, err := sql.Open("mysql", ds)
	if err != nil {
		return nil, err
	}

	return &ShardConnection{
		User:     user,
		Addr:     addr,
		DbName:   db,
		dataBase: dataBase,
	}, nil
}

func toDataSource(user, addr, db string) string {
	return fmt.Sprintf("%s@tcp(%s)/%s", user, addr, db)
}

func (s *ShardConnection) Query(sqlText string, args ...interface{}) ([]*protos.Row, error) {
	_, rows, err := s.query(sqlText, args...)
	if err != nil {
		return nil, err
	}

	var resultRows []*protos.Row
	for _, row := range rows {
		var columns []*protos.Column
		for i := range columns {
			var value *wrappers.StringValue
			col := row[i]
			if col != nil {
				val := string(col)
				value = &wrappers.StringValue{Value: val}
			}
			columns = append(columns, &protos.Column{Value: value})
		}
		resultRows = append(resultRows, &protos.Row{Columns: columns})
	}

	return resultRows, nil
}

func (s *ShardConnection) QueryQuery(q *Query) ([]string, [][]sql.RawBytes, error) {
	return s.query(q.Sql, q.Args...)
}

func (s *ShardConnection) query(sqlText string, args ...interface{}) ([]string, [][]sql.RawBytes, error) {
	queryRows, err := s.dataBase.Query(sqlText, args...)
	if err != nil {
		return nil, nil, err
	}

	return s.scanRows(queryRows)
}

func (s *ShardConnection) scanRows(rows *sql.Rows) ([]string, [][]sql.RawBytes, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var scannedRows [][]sql.RawBytes
	for rows.Next() {
		values := make([]sql.RawBytes, len(columnNames))

		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, nil, err
		}

		var cols []sql.RawBytes
		for _, col := range values {
			cols = append(cols, col)
		}
		scannedRows = append(scannedRows, cols)
	}

	return columnNames, scannedRows, nil
}

func (s *ShardConnection) Exec(sqlText string, args ...interface{}) (sql.Result, error) {
	log.Printf("%s args:%+v", sqlText, args)
	result, err := s.dataBase.Exec(sqlText, args...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ShardConnection) ExecStrictQueries(queries []*Query) error {
	tx, err := s.dataBase.Begin()
	if err != nil {
		return err
	}

	rollback := func(err error) error {
		tx.Rollback()
		return err
	}

	sessionQuery := `SET
		SESSION time_zone = '+00:00',
		sql_mode = CONCAT(@@session.sql_mode, ',STRICT_ALL_TABLES')
		`
	if _, err := tx.Exec(sessionQuery); err != nil {
		return rollback(err)
	}

	for _, query := range queries {
		log.Printf("query: %s, args: %+v\n", query.Sql, query.Args)
		if _, err := tx.Exec(query.Sql, query.Args...); err != nil {
			err = errors.Wrap(err, fmt.Sprintf("query=%s, args=%+v", query.Sql, query.Args))
			return rollback(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *ShardConnection) Close() error {
	return s.dataBase.Close()
}
