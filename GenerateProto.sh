#!/bin/sh
#protoc --go_out=pkg/cache ./proto/**/*.proto
#protoc -I=proto --go_out=pkg/cache/proto ./proto/declare/*.proto
for x in proto/**/*.proto; do protoc --go_out=paths=source_relative:pkg/cache $x; done