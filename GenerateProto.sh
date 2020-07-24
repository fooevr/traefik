#!/bin/sh
#protoc --go_out=pkg/cache ./proto/**/*.proto
#protoc -I=proto --go_out=pkg/cache/proto ./proto/declare/*.proto
#for x in proto/**/*.proto; do
#  protoc --go_out=plugins=grpc,paths=source_relative:pkg/cache $x;
#  done
rm -rf pkg/cache/proto
mkdir pkg/cache/proto
protoc -I proto/ proto/**/*.proto --go-grpc_out=pkg/cache --go_out=pkg/cache