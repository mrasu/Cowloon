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
	resetShards("cluster1", "1", "cluster2", "1")
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

func resetShards(key1, shard1, key2, shard2 string) {
	conn, err := grpc.Dial("localhost:15501", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	c := protos.NewUserMessageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	s1 := &protos.KeyData{
		Key:       key1,
		ShardName: shard1,
	}
	s2 := &protos.KeyData{
		Key:       key2,
		ShardName: shard2,
	}
	_, err = c.RemoveKey(ctx, s1)
	if err != nil {
		fmt.Println(err)
	}
	_, err = c.RemoveKey(ctx, s2)

	_, err = c.RegisterKey(ctx, s1)
	if err != nil {
		panic(err)
	}
	_, err = c.RegisterKey(ctx, s2)
	if err != nil {
		panic(err)
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
