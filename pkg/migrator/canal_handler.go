package migrator

import (
	"fmt"
	"strconv"

	"github.com/mrasu/Cowloon/pkg/migrator/tableinfo"

	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/siddontang/go-mysql/canal"
)

type CanalHandler struct {
	canal.DummyEventHandler
	dbName       string
	dmlEventChan chan []*db.Query

	migratingTables []*tableinfo.MigrationTable
}

func NewCanalHandler(dbName string, dc chan []*db.Query) *CanalHandler {
	return &CanalHandler{
		dbName:       dbName,
		dmlEventChan: dc,

		migratingTables: []*tableinfo.MigrationTable{},
	}
}

func (c *CanalHandler) AddTable(t *tableinfo.MigrationTable) {
	c.migratingTables = append(c.migratingTables, t)
}

func (c *CanalHandler) OnRow(ev *canal.RowsEvent) error {
	if ev.Table.Schema != c.dbName {
		return nil
	}

	queries, err := c.toQuery(ev)
	if err != nil {
		return err
	}

	if len(queries) > 0 {
		c.dmlEventChan <- queries
	}

	return nil
}

func (c *CanalHandler) toQuery(ev *canal.RowsEvent) ([]*db.Query, error) {
	var queries []*db.Query
	if ev.Action == canal.InsertAction {
		mt := c.matchedMigrationTable(ev)
		if mt == nil {
			return []*db.Query{}, nil
		}

		rows, err := c.filterTargetRows(ev, mt)
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

		mt := c.matchedMigrationTable(ev)
		if mt == nil {
			return []*db.Query{}, nil
		}

		ki, err := c.getKeyColumnIndex(ev, mt)
		if err != nil {
			return nil, err
		}

		isT, err := c.isTarget(mt, ev.Rows[0][ki])
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
		mt := c.matchedMigrationTable(ev)
		if mt == nil {
			return []*db.Query{}, nil
		}

		rows, err := c.filterTargetRows(ev, mt)
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

func (c *CanalHandler) matchedMigrationTable(ev *canal.RowsEvent) *tableinfo.MigrationTable {
	for _, t := range c.migratingTables {
		if ev.Table.Name == t.Name {
			return t
		}
	}

	return nil
}

func (c *CanalHandler) filterTargetRows(ev *canal.RowsEvent, t *tableinfo.MigrationTable) ([][]interface{}, error) {
	ki, err := c.getKeyColumnIndex(ev, t)
	if err != nil {
		return nil, err
	}

	var rows [][]interface{}
	for _, r := range ev.Rows {
		isT, err := c.isTarget(t, r[ki])
		if err != nil {
			return nil, err
		}

		if isT {
			rows = append(rows, r)
		}
	}

	return rows, nil
}

func (c *CanalHandler) getKeyColumnIndex(ev *canal.RowsEvent, t *tableinfo.MigrationTable) (int, error) {
	ki := -1
	for i, co := range ev.Table.Columns {
		if co.Name == t.MigrationTargetColumnName {
			ki = i
			break
		}
	}

	if ki == -1 {
		return -1, fmt.Errorf(t.MigrationTargetColumnName+" doesn't exist at %v", ev)
	}

	return ki, nil
}

func (c *CanalHandler) isTarget(t *tableinfo.MigrationTable, columnValue interface{}) (bool, error) {
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

	return t.MigrationTargetColumn.IsMigrationTarget(key), nil
}
