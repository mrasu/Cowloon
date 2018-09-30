package migrator

import (
	"fmt"
	"strconv"

	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/siddontang/go-mysql/canal"
)

type CanalHandler struct {
	canal.DummyEventHandler
	dbName       string
	dmlEventChan chan []*db.Query

	targetKey string
}

func NewCanalHandler(dbName string, dc chan []*db.Query, key string) *CanalHandler {
	return &CanalHandler{
		dbName:       dbName,
		dmlEventChan: dc,

		targetKey: key,
	}
}

func (c *CanalHandler) OnRow(ev *canal.RowsEvent) error {
	if ev.Table.Schema != c.dbName {
		return nil
	}

	queries, err := c.toQuery(ev)
	if err != nil {
		return err
	}

	c.dmlEventChan <- queries
	return nil
}

func (c *CanalHandler) toQuery(ev *canal.RowsEvent) ([]*db.Query, error) {
	var queries []*db.Query
	if ev.Action == canal.InsertAction {
		rows, err := c.filterTargetRows(ev)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			query, err := db.BuildInsertQuery(ev.Table.Name, ev.Table.Columns, row)
			if err != nil {
				return nil, err
			}
			queries = append(queries, query)
		}
	} else if ev.Action == canal.UpdateAction {
		if len(ev.Rows) != 2 {
			return nil, fmt.Errorf("update event doesn't have two rows (rows: %d)", len(ev.Rows))
		}

		ki, err := c.getKeyColumnIndex(ev)
		if err != nil {
			return nil, err
		}

		isT, err := c.isTarget(ev.Rows[0][ki])
		if err != nil {
			return nil, err
		}

		if isT {
			query, err := db.BuildUpdateQuery(ev.Table.Name, ev.Table.Columns, ev.Rows[0], ev.Rows[1])
			if err != nil {
				return nil, err
			}
			queries = append(queries, query)
		}
	} else if ev.Action == canal.DeleteAction {
		rows, err := c.filterTargetRows(ev)
		if err != nil {
			return nil, err
		}

		for _, row := range rows {
			query, err := db.BuildDeleteQuery(ev.Table.Name, ev.Table.Columns, row)
			if err != nil {
				return nil, err
			}
			queries = append(queries, query)
		}
	}

	return queries, nil
}

func (c *CanalHandler) filterTargetRows(ev *canal.RowsEvent) ([][]interface{}, error) {
	ki, err := c.getKeyColumnIndex(ev)
	if err != nil {
		return nil, err
	}

	var rows [][]interface{}
	for _, r := range ev.Rows {
		isT, err := c.isTarget(r[ki])
		if err != nil {
			return nil, err
		}

		if isT {
			rows = append(rows, r)
		}
	}

	return rows, nil
}

func (c *CanalHandler) getKeyColumnIndex(ev *canal.RowsEvent) (int, error) {
	ki := -1
	for i, c := range ev.Table.Columns {
		if c.Name == keyColumnName {
			ki = i
			break
		}
	}

	if ki == -1 {
		return -1, fmt.Errorf(keyColumnName+" doesn't exist at %v", ev)
	}

	return ki, nil
}

func (c *CanalHandler) isTarget(columnValue interface{}) (bool, error) {
	var key string
	switch c := columnValue.(type) {
	case int:
		key = strconv.Itoa(c)
	case int32:
		key = strconv.FormatInt(int64(c), 10)
	case int64:
		key = strconv.FormatInt(c, 10)
	case string:
		key = c
	default:
		return false, fmt.Errorf("invalid type: %v", c)
	}

	return key == c.targetKey, nil
}
