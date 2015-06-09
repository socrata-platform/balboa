
clean:
	cd ../ && sbt balboa-ticker-tape/clean

assembly:
	cd ../ && sbt balboa-ticker-tape/assembly

docker: assembly
	cp `ls -1t target/scala-2.10/*.jar | sed -n 1p` docker/balboa-ticker-tape-assembly.jar
	cd docker/ && docker build .
