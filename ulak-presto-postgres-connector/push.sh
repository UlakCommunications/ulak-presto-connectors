 VERSION=$1
 set -x
 ./mvnw clean package
 docker build -t 192.168.57.202:35000/ulak/trino-postgres:${VERSION} .
 docker push     192.168.57.202:35000/ulak/trino-postgres:${VERSION}