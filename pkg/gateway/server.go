package gateway

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mrasu/Cowloon/pkg/protos"
	"errors"
)

type Server struct {

}

func (s *Server) SendSql(ctx context.Context, in *protos.SqlRequest) (*protos.SqlResponse, error) {
	if in.Key == "" {
		return nil, errors.New("key is empty")
	}

	fmt.Println("EXECUTE SQL!!!!", in.Sql)
	d, err := getDb(in.Key)
	if err != nil {
		return nil, err
	}
	rows, err := d.execSql(in.Sql)
	if err != nil {
		return nil, err
	}

	return &protos.SqlResponse{Rows: rows}, nil
}
