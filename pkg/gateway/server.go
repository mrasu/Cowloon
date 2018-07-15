package gateway

import (
	"context"
	"fmt"

	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mrasu/Cowloon/pkg/protos"
)

const (
	ErrorNum = -2147483648
)

type Server struct {
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

func (s *Server) selectDb(in *protos.SqlRequest) (*Db, error) {
	if in.Key == "" {
		return nil, errors.New("key is empty")
	}

	r, err := NewRouter()
	if err != nil {
		return nil, err
	}

	d, err := r.GetDb(in.Key)
	if err != nil {
		return nil, err
	}

	return d, nil
}
