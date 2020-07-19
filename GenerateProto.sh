#!/bin/sh
#protoc --go_out=pkg/cache ./proto/**/*.proto
#protoc -I=proto --go_out=pkg/cache/proto ./proto/declare/*.proto
for x in proto/**/*.proto; do protoc --go_out=plugins=grpc,paths=source_relative:pkg/cache $x; done
#protoc -I proto/ proto/**/*.proto --go_out=plugins=grpc,paths=source_relative:pkg/cache