# Balboa-HTTP Docker

This Docker configuration for Balboa-HTTP

To build the image run:

```bash
sbt clean balboa-http/assembly
cp balboa-http/target/scala-2.11/balboa-http-assembly-*.jar balboa-http/docker/balboa-http-assembly.jar
docker build --rm -t balboa-http balboa-http/docker
```

## Required:

* CASSANDRAS: Comma separated list of Cassandra nodes (host:port,host:port,...).
* CASSANDRA_KEYSPACE: Cassandra Key Space to persist metrics into IE. Metrics2012
* LOG_LEVEL= The log output level. IE: DEBUG, INFO

### Cassandra
When running Cassandra locally, be sure that the address in the `env/local`
file is reachable from your docker container. Note that since docker runs
in a VM on OSX, `127.0.0.1` will not reach a local Cassandra instance.
Further, make sure that Cassandra is listening for CQL connecitons on the
same address (configurable via the `rpc_address` variable in the Cassandra
config, commonly located at `/usr/local/etc/cassandra/cassandra.yaml`).

## Configure your environment.

To run locally update the `env/local` or copy it to a new file.  Place your IP
into the new or updated configuration file.

## Running your container

`docker run --env-file=<full path to your envfile> -p 2012:2012 -t balboa-http`

References:
* [How to Docker](https://docs.google.com/a/socrata.com/document/d/1pSYyuf32tr-eLF6HRtBIZI5fMhpHGwrkFldsGU6F9uI/edit#heading=h.v3c2hdhylifl)
* [AutoDeploy Dockerized Projects](https://docs.google.com/a/socrata.com/document/d/1MWF-8ZJKNaurcwAuJLjcjhgYHOXDLWkKGyBGnWJdM_A/edit#)



