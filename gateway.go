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
	m, err := gateway.NewManager()
	if err != nil {
		panic(err)
	}
	go runServer(m)
	migrateShard(m)
}

func runServer(m *gateway.Manager) {
	lis, err := net.Listen("tcp", port)
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

func migrateShard(m *gateway.Manager) {
	err := m.MigrateShard("4", "shard1")
	if err != nil {
		panic(err)
	}
}
