package main

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/mrasu/Cowloon/pkg/protos"
	"google.golang.org/grpc"
)

func main() {
	runQueries()
}

func etcd() {
	cfg := clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	}

	c, err := clientv3.New(cfg)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	resp, err := c.Get(context.Background(), "key")
	if err != nil {
		panic(err)
	}

	for _, kv := range resp.Kvs {
		fmt.Printf("%s: %s\n", kv.Key, kv.Value)
	}
}

func runQueries() {
	fmt.Println("Execute: cluster1")
	runQuery("cluster1")
	fmt.Println("========")
	fmt.Println("Execute: cluster2")
	runQuery("cluster2")
}

func runQuery(key string) {
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
		Key: key,
	}
	r, err := c.SendSql(ctx, req)
	if err != nil {
		panic(err)
	}

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
