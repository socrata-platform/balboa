# Balboa Agent

Balboa Agents are standalone processes transport metrics that were written to
disk locally to the metrics data store.

## Installation

This project is published as a maven repository and can be utilized and
referenced as such through socrata's public release library.

 SBT:
 1. Add the Socrata release repository to the project's `build.sbt`:

 ```
  resolvers := Seq(
    "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release"
  )
 ```

2. Add the balboa-client dependency to the project's library dependencies:

  ```
  libraryDependencies ++= Seq(
    ...
    "com.socrata" % "balboa-agent" %% "0.17.+",
    ...
  )
 ```

Others: TODO

## Usage and Code Example

### Terminology

* _agent_ : Standalone process that both consumes and emits metrics.
* _amq_ : Active MQ

### Build

Balboa Agent can built into an executable Assembly (Fat) Jar.

```
sbt balboa-agent/assembly
```

This project also incorporates [Sbt Native
Packager](http://www.scala-sbt.org/sbt-native-packager/). And builds a simple
debian package that wraps an assembly jar. This assembly jar can be found at
the debian install root /balboa-agent/bin/balboa-agent-assembly.jar.

#### Building Debian Distributions

```
sbt balboa-agent/debian:packageBin
```

This packages a debian file at
balboa-agent/target/balboa-agent_VERSION_ARCHTYPE.deb. Currently debian is
build with ARCHTYPE = all implying that it can run on all linux distros
supporting debian.

#### Building Docker Containers (In Progress)

Requirements
* docker

Completed
* End to End docker container composition with sbt-native-package, socrata/java
  base image, and socrata user

In Progress
* Configuring docker registry to support `sbt balboa-agent/docker:publish`
* Ensuring SBT-Native-Pacakager and Socrata Ship.d play nice.


Currently docker can be built and published to the local docker server via...
```
sbt balboa-agent/docker:publishLocal
```

Publish Docker to a remote docker repository
```
sbt balboa-agent/docker:publishLocal
```

Stage the docker file and related contents locally at
balboa-agent/target/docker/stage
```
sbt balboa-agent/docker:stage
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

### Running out of the box

`cd balboa && sbt assembly`
`sbt balboa-agent/run "Command line arguments"`

## Tests

### Unit Tests

Standard SBT Tests.

```
sbt balboa-agent/test
```

### Integration Tests

sbt balboa-agent/run "-Dbalboa.config=balboa-agent/src/main/resources/config/config.properties"
sbt balboa-agent/it:test

* Now, the integration tests should be able to write to the file system and
  read from the queue.

## History

Balboa-agent formerly the Metric Consumer started off as a one off project to
help ingest and consolidate metrics on individual servers. This helped manage
the amount of traffic being pushed through activemq.

## License

See root project license.
