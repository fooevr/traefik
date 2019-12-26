#!/bin/sh
protoc --proto_path=proto --go_out=pkg/cache/proto proto/*.proto