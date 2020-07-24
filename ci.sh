#!/bin/sh

cd cmd/traefik
$GOPATH/bin/gox --osarch="linux/amd64"

cd ../../Traefik-gRPC
#cp ../cmd/docker/docker_linux_amd64  ./dockerEnv
cp ../cmd/traefik/traefik_linux_amd64 ./traefik
rm -rf ./conf
cp -r ../conf ./conf/
cp ../traefik.sample.toml ./traefik.toml
#cp -r ../defaultProtos  ./defaultProtos
docker build -t repo.chinaacdm.com/library/gateway:1.0.0 .
