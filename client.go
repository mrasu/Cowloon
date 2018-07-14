package main

import (
	"fmt"
	"google.golang.org/grpc"
	"context"
	"time"
	"github.com/mrasu/Cowloon/pkg/protos"
)

func main() {
	conn, err := grpc.Dial("localhost:15501", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := protos.NewUserMessageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &protos.SqlRequest{
		Sql: "SELECT id, text FROM messages",
		Key: "2",
	}
	r, err := c.SendSql(ctx, req)
	if err != nil {
		panic(err)
	}
	fmt.Println("*********")

	for _, row := range r.Rows {
		for _, column := range row.Columns {
			if column.Value == nil {
				fmt.Println(nil)
			} else {
				fmt.Println(column.Value.Value)
			}
		}
		fmt.Println("*********")
	}
}

