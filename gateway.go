package main

import (
	"fmt"
	"net"

	"github.com/mrasu/Cowloon/pkg/gateway"
	"github.com/mrasu/Cowloon/pkg/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":15501"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}

	s, err := gateway.NewServer()
	if err != nil {
		panic(err)
	}

	gs := grpc.NewServer()
	protos.RegisterUserMessageServer(gs, s)
	reflection.Register(gs)

	if err = gs.Serve(lis); err != nil {
		panic(err)
	}
	// runUpdate()
}

func runUpdate() {
	db, err := gateway.NewDb("root@tcp(127.0.0.1:13307)/cowloon")
	if err != nil {
		panic(err)
	}

	rows, err := db.Exec(`UPDATE messages SET text = CONCAT(text, "COOLA") WHERE id = 2`)
	if err != nil {
		panic(err)
	}
	fmt.Println(rows)
}
