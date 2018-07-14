package gateway

import (
	"database/sql"
	"github.com/mrasu/Cowloon/pkg/protos"
	"github.com/golang/protobuf/ptypes/wrappers"
	"sync"
)

const(
	dataSource1 = "root@tcp(127.0.0.1:13306)/cowloon"
	dataSource2 = "root@tcp(127.0.0.1:13307)/cowloon"
)

type Db struct {
	datasource string
	db *sql.DB
}

var dbs = map[int64]*Db{}
var mu sync.RWMutex

func getDb(key string) (*Db, error) {
	lastChar := key[len(key)-1]
	dbIndex := int64(lastChar) & 1

	mu.RLock()
	db, ok := dbs[dbIndex]
	mu.RUnlock()

	if ok {
		return db, nil
	}

	var ds string

	if int64(lastChar) & 1 == 1 {
		ds =  dataSource1
	} else {
		ds = dataSource2
	}

	db, err := newDb(ds)
	if err != nil {
		return nil, err
	}

	mu.Lock()
	dbs[dbIndex] = db
	mu.Unlock()
	return db, nil
}

func newDb(ds string) (*Db, error) {
	db, err := sql.Open("mysql", ds)
	if err != nil {
		return nil, err
	}

	return &Db{
		datasource: ds,
		db: db,
	}, nil
}


func (d *Db) execSql(sqlText string) ([]*protos.Row, error){
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
