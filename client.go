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
	// execQuery("cluster1")
	resetShards()
	fmt.Println("========")
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

func resetShards() {
	k2sList := [][]string{
		{"1", "shard1"},
		{"2", "shard1"},
		{"3", "shard2"},
		{"4", "shard2"},
	}

	conn, err := grpc.Dial("localhost:15501", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := protos.NewUserMessageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, k2s := range k2sList {
		d := &protos.KeyData{
			Key:       k2s[0],
			ShardName: k2s[1],
		}

		_, err = c.RemoveKey(ctx, d)
		if err != nil {
			fmt.Println(err)
		}

		_, err = c.RegisterKey(ctx, d)
		if err != nil {
			panic(err)
		}
	}
}

func runQueries() {
	for i := 0; i < 10; i++ {
		fmt.Println("Execute: shard1")
		runQuery("1")
		fmt.Println("========")
		fmt.Println("Execute: shard2")
		runQuery("2")
		time.Sleep(time.Second)
	}
}

func runQuery(key string) {
	conn, err := grpc.Dial("localhost:15501", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := protos.NewUserMessageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &protos.SqlRequest{
		Sql: "SELECT id, text FROM messages",
		Key: key,
	}
	r, err := c.Query(ctx, req)
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

func execQuery(key string) {
	conn, err := grpc.Dial("localhost:15501", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := protos.NewUserMessageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req := &protos.SqlRequest{
		Sql: `UPDATE messages SET text = CONCAT(text, "COOLA") WHERE id = 2`,
		Key: key,
	}
	r, err := c.Exec(ctx, req)
	if err != nil {
		panic(err)
	}

	fmt.Printf("RowsAffected: %d\n", r.RowsAffected)
	fmt.Printf("LastInsertedId: %d\n", r.LastInsertedId)
}
