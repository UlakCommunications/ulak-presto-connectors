#!/bin/bash
VERSION=$1
platform=$2
prod=$3
set -x

#docker pull  trinodb/trino:432
 ./mvnw clean package

#docker buildx create  --use --config=../../buildx_config.toml
#DOCKER_BUILDKIT=0 
docker buildx use mybuilder
docker buildx inspect --bootstrap

nexus_repo="192.168.57.202:35000/maya/trino:${VERSION}"

# Eğer prod seçilmişse, prod_nexus_repo ve nexus_repo push işlemi gerçekleşir
if [ "$prod" == "true" ]; then
    prod_nexus_repo="192.168.27.6:35000/maya/trino:${VERSION}"

    # 1- Eğer prod true ise, sadece Nexus repo'yu baz alarak build işlemi yapılır
    echo "Prod seçildi, sadece Nexus repo'yu baz alarak build yapılıyor."


docker buildx build --no-cache --output=type=registry,registry.insecure=true --platform=${platform}   -f Dockerfile --add-host=maya-nexus.ulakhaberlesme.com.tr:192.168.13.47   --progress plain -t  $nexus_repo .  --push


    docker pull $nexus_repo
    docker tag $nexus_repo $prod_nexus_repo
    docker push $prod_nexus_repo

else
    # 4- Eğer prod false ise, sadece Nexus repo'yu baz alarak build işlemi yapılır
    echo "Prod seçilmedi, sadece Nexus repo'yu baz alarak build yapılıyor."

docker buildx build --no-cache --output=type=registry,registry.insecure=true --platform=${platform}   -f Dockerfile --add-host=maya-nexus.ulakhaberlesme.com.tr:192.168.13.47   --progress plain -t  $nexus_repo .  --push

fi

