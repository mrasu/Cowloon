package main

import (
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
	// runServer()
	readBinlog2()
}

func runServer() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}

	m, err := gateway.NewManager()
	if err != nil {
		panic(err)
	}

	gs := grpc.NewServer()
	protos.RegisterUserMessageServer(gs, m)
	reflection.Register(gs)

	if err = gs.Serve(lis); err != nil {
		panic(err)
	}
}

func readBinlog2() {
	m, err := gateway.NewManager()
	if err != nil {
		panic(err)
	}
	err = m.MigrateShard("4", "shard1")
	if err != nil {
		panic(err)
	}
}
