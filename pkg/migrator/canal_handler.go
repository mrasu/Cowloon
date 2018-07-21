package migrator

import (
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

	queries := c.toQuery(ev)
	c.dmlEventChan <- queries
	return nil
}

func (c *CanalHandler) toQuery(ev *canal.RowsEvent) []*db.Query {
	var queries []*db.Query
	if ev.Action == canal.InsertAction {
		for _, row := range ev.Rows {
			queries = append(queries, db.BuildInsertQuery(ev.Table.Name, ev.Table.Columns, row))
		}
	}

	return queries
}
