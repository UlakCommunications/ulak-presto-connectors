 VERSION=-qw
 ./mvnw clean package
 docker build -t 192.168.57.202:35000/trinodb/trino:432${VERSION} .
 docker push 192.168.57.202:35000/trinodb/trino:432${VERSION}