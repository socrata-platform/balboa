# balboa-http

balboa-http is the service that wraps the metrics persistence store, Cassandra.
It provides a simple, RESTful interface for reading stored metrics.

## Build

balboa-http can built into an executable Assembly (Fat) Jar.

```
sbt balboa-http/assembly
```

### Configuration

Configuration is done through properties files or via the
command line. Command line arguments overwrite any found file based
configuration properties.

Properties files are taken from these places, in this order:

1. Off the classpath `config/config.properties`. This is where the defaults
   come from, and this file is built into the jar.
2. JVM parameter, `-Dbalboa.config` if it is provided.
3. `/etc/balboa.properties` if it exists.

Example configuration can be found in `config/config.properties`.

### Running Service Locally

`sbt "balboa-http/run [port-for-balboa-http-to-listen-on]"`

## Tests

### Unit Tests

Standard SBT Tests.

```
sbt balboa-http/test
```

### Integration Tests

```
sbt balboa-http/run "-Dbalboa.config=balboa-agent/src/main/resources/config/config.properties"
sbt balboa-http/it:test
```

### Load Tests

balboa-http includes a set of load tests using [Gatling](http://gatling.io/#/).
These live in balboa-http/src/it/scala/com.socrata.balboa.server/load.
Each file implements a Gatling simulation designed to load test a
particular aspect of balboa-http. After the tests have been run, the
commandline output wil include the URL of a local file where the results
may be viewed.

```
sbt balboa-http/run "-Dbalboa.config=balboa-agent/src/main/resources/config/config.properties"
sbt gatling-it:test
```

