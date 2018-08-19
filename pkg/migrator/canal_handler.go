package migrator

import (
	"fmt"

	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/siddontang/go-mysql/canal"
)

type CanalHandler struct {
	canal.DummyEventHandler
	dbName       string
	dmlEventChan chan []*db.Query
}

func NewCanalHandler(dbName string, dc chan []*db.Query) *CanalHandler {
	return &CanalHandler{
		dbName:       dbName,
		dmlEventChan: dc,
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
		for _, row := range ev.Rows {
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

		query, err := db.BuildUpdateQuery(ev.Table.Name, ev.Table.Columns, ev.Rows[0], ev.Rows[1])
		if err != nil {
			return nil, err
		}
		queries = append(queries, query)
	} else if ev.Action == canal.DeleteAction {
		for _, row := range ev.Rows {
			query, err := db.BuildDeleteQuery(ev.Table.Name, ev.Table.Columns, row)
			if err != nil {
				return nil, err
			}
			queries = append(queries, query)
		}
	}

	return queries, nil
}
