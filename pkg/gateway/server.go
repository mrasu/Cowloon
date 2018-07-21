package gateway

import (
	"context"
	"fmt"

	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mrasu/Cowloon/pkg/db"
	"github.com/mrasu/Cowloon/pkg/migrator"
	"github.com/mrasu/Cowloon/pkg/protos"
)

const (
	ErrorNum = -2147483648
)

type Server struct {
	router *Router
}

func NewServer() (*Server, error) {
	r, err := NewRouter()
	if err != nil {
		return nil, err
	}

	return &Server{
		router: r,
	}, nil
}

func (s *Server) Query(ctx context.Context, in *protos.SqlRequest) (*protos.QueryResponse, error) {
	d, err := s.selectDb(in)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Query SQL!!!!: key: %s, sql: `%s`\n", in.Key, in.Sql)
	rows, err := d.Query(in.Sql)
	if err != nil {
		return nil, err
	}

	return &protos.QueryResponse{Rows: rows}, nil
}

func (s *Server) Exec(ctx context.Context, in *protos.SqlRequest) (*protos.ExecResponse, error) {
	d, err := s.selectDb(in)
	if err != nil {
		return nil, err
	}

	fmt.Printf("EXEC SQL!!!!: key: %s, sql: `%s`\n", in.Key, in.Sql)
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

func (s *Server) selectDb(in *protos.SqlRequest) (*db.ShardConnection, error) {
	if in.Key == "" {
		return nil, errors.New("key is empty")
	}

	sc, err := s.router.GetShardConnection(in.Key)
	if err != nil {
		return nil, err
	}

	return sc, nil
}

func (s *Server) RegisterKey(ctx context.Context, in *protos.KeyData) (*protos.SimpleResult, error) {
	err := s.router.RegisterKey(in.Key, in.ShardName)
	if err != nil {
		return nil, err
	}

	return &protos.SimpleResult{
		Success: true,
		Message: "Success",
	}, nil
}

func (s *Server) RemoveKey(ctx context.Context, in *protos.KeyData) (*protos.SimpleResult, error) {
	err := s.router.RemoveKey(in.Key)
	if err != nil {
		return nil, err
	}

	return &protos.SimpleResult{
		Success: true,
		Message: "Success",
	}, nil
}

func (s *Server) MigrateShard(fromKey, toKey string) error {
	fromS, err := s.router.GetShardConnection(fromKey)
	if err != nil {
		return err
	}
	toS, err := s.router.GetShardConnection(toKey)

	a := migrator.NewApplier(fromS, toS)
	err = a.Run()
	if err != nil {
		return err
	}

	return nil
}
