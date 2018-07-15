package gateway

import (
	"context"
	"fmt"

	"errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mrasu/Cowloon/pkg/protos"
)

type Server struct {
}

func (s *Server) SendSql(ctx context.Context, in *protos.SqlRequest) (*protos.SqlResponse, error) {
	if in.Key == "" {
		return nil, errors.New("key is empty")
	}

	fmt.Printf("EXECUTE SQL!!!!: key: %s, sql: `%s`\n", in.Key, in.Sql)
	r, err := NewRouter()
	if err != nil {
		return nil, err
	}

	d, err := r.GetDb(in.Key)
	if err != nil {
		return nil, err
	}
	rows, err := d.execSql(in.Sql)
	if err != nil {
		return nil, err
	}

	return &protos.SqlResponse{Rows: rows}, nil
}
