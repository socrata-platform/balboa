assembly:
	cd ../ && sbt balboa-jms/assembly

docker: assembly
	cp `ls -1t target/scala-2.10/*.jar | sed -n 1p` docker/balboa-service-jms-assembly.jar
	docker build -t balboa-service-jms docker/
