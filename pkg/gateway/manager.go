package gateway

import (
	"context"
	"fmt"

	"github.com/mrasu/Cowloon/pkg/migrator"

	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/mrasu/Cowloon/pkg/protos"
)

const (
	ErrorNum = -2147483648
)

type Manager struct {
	router *Router
}

func NewManager() (*Manager, error) {
	r, err := NewRouter()
	if err != nil {
		return nil, err
	}

	return &Manager{
		router: r,
	}, nil
}

func (m *Manager) Query(ctx context.Context, in *protos.SqlRequest) (*protos.QueryResponse, error) {
	d, err := m.selectDb(in)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Query SQL!!!!: key: %m, sql: `%m`\n", in.Key, in.Sql)
	rows, err := d.Query(in.Sql)
	if err != nil {
		return nil, err
	}

	return &protos.QueryResponse{Rows: rows}, nil
}

func (m *Manager) Exec(ctx context.Context, in *protos.SqlRequest) (*protos.ExecResponse, error) {
	d, err := m.selectDb(in)
	if err != nil {
		return nil, err
	}

	fmt.Printf("EXEC SQL!!!!: key: %m, sql: `%m`\n", in.Key, in.Sql)
	result, err := d.Exec(in.Sql)
	if err != nil {
		return nil, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		// Not raise error when db doesn't support RowsAffected
		rows = ErrorNum
	}
	lId, err := result.LastInsertId()
	if err != nil {
		// Not raise error when db doesn't support LastInsertId
		lId = ErrorNum
	}
	resp := &protos.ExecResponse{
		RowsAffected:   rows,
		LastInsertedId: lId,
	}
	return resp, nil
}

func (m *Manager) selectDb(in *protos.SqlRequest) (*db.ShardConnection, error) {
	if in.Key == "" {
		return nil, errors.New("key is empty")
	}

	sc, err := m.router.GetShardConnection(in.Key)
	if err != nil {
		return nil, err
	}

	return sc, nil
}

func (m *Manager) RegisterKey(ctx context.Context, in *protos.KeyData) (*protos.SimpleResult, error) {
	err := m.router.RegisterKey(in.Key, in.ShardName)
	if err != nil {
		return nil, err
	}

	return &protos.SimpleResult{
		Success: true,
		Message: "Success",
	}, nil
}

func (m *Manager) RemoveKey(ctx context.Context, in *protos.KeyData) (*protos.SimpleResult, error) {
	err := m.router.RemoveKey(in.Key)
	if err != nil {
		return nil, err
	}

	return &protos.SimpleResult{
		Success: true,
		Message: "Success",
	}, nil
}

func (m *Manager) MigrateShard(key, toShardName string) error {
	fromS, err := m.router.GetShardConnection(key)
	if err != nil {
		return err
	}
	toS, err := m.router.buildDb(toShardName)
	if err != nil {
		return err
	}

	a := migrator.NewApplier(key, fromS, toS)
	err = a.Run()
	if err != nil {
		return err
	}

	return nil
}
