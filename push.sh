#!/bin/bash
VERSION=$1
platform=$2
set -x

#docker pull  trinodb/trino:432
 ./mvnw clean package

docker buildx create  --use --config=../../buildx_config.toml
DOCKER_BUILDKIT=0 docker buildx build --no-cache --output=type=registry,registry.insecure=true --platform=${platform}   -f Dockerfile --add-host=maya-nexus.ulakhaberlesme.com.tr:192.168.13.47   --progress plain -t  192.168.57.202:35000/maya/trino:${VERSION} .  --push
