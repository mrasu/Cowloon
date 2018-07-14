#!/usr/bin/env bash

protoc -I pkg/protos/ --proto_path=../../golang/protobuf/ptypes/wrappers pkg/protos/user_message.proto --go_out=plugins=grpc:pkg/protos
