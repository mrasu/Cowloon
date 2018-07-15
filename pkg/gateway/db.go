package gateway

import (
	"database/sql"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/mrasu/Cowloon/pkg/protos"
)

type Db struct {
	datasource string
	db         *sql.DB
}

func NewDb(ds string) (*Db, error) {
	db, err := sql.Open("mysql", ds)
	if err != nil {
		return nil, err
	}

	return &Db{
		datasource: ds,
		db:         db,
	}, nil
}

func (d *Db) execSql(sqlText string) ([]*protos.Row, error) {
	rows, err := d.db.Query(sqlText)
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var resultRows []*protos.Row

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var columns []*protos.Column
		for _, col := range values {
			var value *wrappers.StringValue
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

func (d *Db) Close() error {
	return d.db.Close()
}
