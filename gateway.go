package main

import (
	"google.golang.org/grpc"
	"github.com/mrasu/Cowloon/pkg/protos"
	"google.golang.org/grpc/reflection"
	"net"
	"github.com/mrasu/Cowloon/pkg/gateway"
)


const(
	port = ":15501"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	protos.RegisterUserMessageServer(s, &gateway.Server{})
	reflection.Register(s)

	if err = s.Serve(lis); err != nil {
		panic(err)
	}
}
