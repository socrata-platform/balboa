Balboa-Kafka-Consumer Docker
======================

This Docker Container is a configurable service that is designed to consume
metrics and pump them into a data store.  This particular container is used
as a base for all balboa related consumption needs.  Therefore, it does not
pertain to all general use cases.  Feel free to copy this template and alter
it to adhere to your needs.

To build the image,fdsa run:

  `docker build -rm -t balboa-kafka-consumer .`

## Required:

* ZOOKEEPER_ENSEMBLE: Comma separated list of Zookeeper nodes (host:port,host:port,...).
IE: "10.0.0.1:2181,10.0.0.2:2181"

* CASSANDRA_ENSEMBLE: Comma separated list of Cassandra nodes (host:port,host:port,...).
IE: 10.98.6.4:9042

* KAFKA_TOPIC: String that represent Kafka Topic.  IE. metrics.tenant

## Optional environment variables:

* SERVICE_ROOT: Service root directory (default: /srv/balboa-kafka-consumer)
* BALBOA_KAFKA_CONSUMER_LOG_ROOT: Root log directory (default: /srv/balboa-kafka/shared/log)
* BALBOA_KAFKA_CONSUMER_MAXMEM:  2048m

## Configure your environment.

Note: There is current work to get Consul up and running. Therefore hardcoding IP:Ports for services will be
(hopefully) going away for a more robust way of service discovery.

To run locally update the `env/local` or copy it to a new file.  Place your IP into the new or updated configuration
file.

You will have to log off any VPN and find your local IP address to use for Cassandra and ZooKeeper.  You will then
have to start Cassandra and Zookeeper locally outside your vm.

## Building your container

* Be attached to AWS VPN

`docker build --rm -t balboa-kafka-consumer <path to docker root IE. balboa-kafka/docker>`

## Running your container

`docker run --env-file=<full path to your envfile> -p 9042:9042`

References:
* [How to Docker](https://docs.google.com/a/socrata.com/document/d/1pSYyuf32tr-eLF6HRtBIZI5fMhpHGwrkFldsGU6F9uI/edit#heading=h.v3c2hdhylifl)
* [AutoDeploy Dockerized Projects](https://docs.google.com/a/socrata.com/document/d/1MWF-8ZJKNaurcwAuJLjcjhgYHOXDLWkKGyBGnWJdM_A/edit#)



