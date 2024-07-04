 VERSION=$1
 ./mvnw clean package
 docker build -t 192.168.57.202:35000/maya/trino:${VERSION} .
 docker push 192.168.57.202:35000/maya/trino:${VERSION}