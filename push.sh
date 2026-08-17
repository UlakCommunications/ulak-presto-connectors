#!/bin/bash
version=$1
platform=$2
prod=$3
prod_ip=$4
set -x

#docker pull  trinodb/trino:432
./mvnw clean install
./mvnw package

#unzip openapi/*.zip -d openapi/plugin/

if [[ "$platform" == *","* ]]; then
    echo "Setting up shared docker buildx builder: mybuilder..."
    docker buildx use mybuilder || {
        echo "mybuilder not found, creating it..."
        docker buildx create --name mybuilder --use
    }
    docker buildx inspect --bootstrap
else
    echo "Single platform build (${platform}): using default host builder..."
    docker buildx use default || true
fi

nexus_repo="192.168.57.202:35000/maya/trino:${version}"

# Eğer prod seçilmişse, prod_nexus_repo ve nexus_repo push işlemi gerçekleşir
if [ "$prod" == "true" ]; then
    if [ -z "$prod_ip" ]; then
        echo "Hata: Prod ortamı için IP adresi verilmedi!"
        exit 1
    fi
    echo $prod_ip
    prod_nexus_repo="${prod_ip}:35000/maya/trino:${version}"

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

