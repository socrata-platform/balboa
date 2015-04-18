# Balboa Dispatcher Client

This project provides an extensible library for publishing metrics to multiple internal clients.

## Motivation

As infrastructure and systems evolve there is a need to be able to deprecate and replace old systems.  One of the key
 requirements is to be able to configure which system to use via configuration.  This project was built to facilitate
  the smooth transition between different messaging buses.

## Installation

This project is published as a maven repository and can be utilized and referenced as such through socrata's public
 release library.

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
   "com.socrata" % "balboa-dispatcher-client" %% "[0.15, )",
   ...
 )
```

Others: TODO

## Usage and Code Example

### Using the existing Metrics Dispatcher

The Metric Dispatcher is fully dependent on the runtime configuration file.  IE. balboa.properties.  The following is
 an example of required properties.  Notice balboa.dispatcher.types value is a comma separated list of 
 
```
 # Declare the desired client types you want to utilize in a comma separated list.
 balboa.dispatcher.types = "jms,kafka"
 # All failed client writes will fall into this directory.  IE. "path/to/directory/jms"
 balboa.emergency.backup.dir = "path/to/directory"
 
 # Client Specific Configuration Details
 
 # Client Type: JMS
 # Where is the activemq server.
 balboa.jms.activemq.server = "server:port"
 # Where is the activemq server.
 balboa.jms.activemq.queue = "queue"

 # Client Type: KAFKA
 # List of Kafka brokers for pulling down metadata
 balboa.kafka.brokers = "server1:port1,server2:port2,server3:port3"
 # Kafka Topic
 balboa.kafka.topic = "some_topic"
```

Include the MetricLoggerToDispatcher trait in your class, object, or other trait.  Then call MetricLogger() to get a 
reference to the MetricLogger.
  
```
class MyContainingClass extends MetricLoggerToDispatcher {

  val logger = MetricLogger()
  logger.logMetric("entity-id", "name", value, time, RecordType.AGGREGATE||RecordType.ABSOLUTE)

}
```

## Tests

Testing is done via ScalaTest

Running test:
```
sbt balboa-dispatcher-client/test
```

## Contributing

Let people know how they can dive into the project, include important links to things like issue trackers, irc, twitter accounts if applicable.

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## History

TODO: Write history

## Credits

TODO: Write credits

## License

A short snippet describing the license (MIT, Apache, etc.)
TODO: Write license
