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
}

func readBinlog2() {
	s, err := gateway.NewServer()
	if err != nil {
		panic(err)
	}
	err = s.MigrateShard("cluster1", "cluster2")
	if err != nil {
		panic(err)
	}
}
