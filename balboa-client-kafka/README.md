# Balboa Kafka Client

This project provides an extensible library for publishing any serializable entity to an existing Kafka Cluster.

## Motivation

Kafka has become a proven distributed messaging bus for real-time data pipelines.  This library provides a mechanism for
publishing messages to Kafka Clusters.  Currently, this project focuses heavily on publishing Balboa Metrics to an existing
 cluster.

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
   "com.socrata" % "balboa-kafka-client" %% "0.14.0",
   ...
 )
```

Others: TODO

## Usage and Code Example

We are currently not allowing the auto creation of topic outside of testing environments to prevent unwanted namespace
 collisions.  We will continue to iterate and devise a way pragmatic process for individual teams to easily create topics
 without namespace collisions.  For now please consult with the Metrics team to create a topic.

In order to begin logging Balboa Metrics all you need to do is include the trait in your wrapper class.  It is considered
generally good practice to utilize the singleton pattern.  Having a minimal number of producers is ideal for minimizing the number
of network connections.

```
trait MyLogger extends MetricLoggerToKafka {

    // To create a logger we require....
    //      A list that is a subset of brokers within a cloud environment.  This will not be needed when consul discovery.
    //      An existing topic for that cluster
    //      And a backup file for failed log
    val logger = MetricLogger("localhost:9062,localhost:9063", "my_existing_metric_topic", "/path/to/backup/file")

    logger.logMetric("entity-id", "name", value, time, RecordType.AGGREGATE||RecordType.ABSOLUTE)
}

```

## Tests

Testing is done as an extension of Apache Kafka KafkaTestingHarness.  It creates instance of Kafka Servers, Producers,
 and Consumers to mock an actual running Kafka environment.

Running test:
```
sbt balboa-kafka-client/test
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
