# Balboa Kafka Client

This project provides an extensible library for publishing any serializable entity to an existing Kafka Cluster.

## Motivation

Kafka has become a proven distributed messaging bus for real-time data pipelines.  This library provides a mechanism for
publishing messages to Kafka Clusters.  Currently, this project focuses heavily on publishing Balboa Metrics to an existing
 cluster.

 We want to provide everyone with the ability to easily ingrain Kafka Producers and Consumers in there services with as little
 learning and dependencies as possible.

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
   "com.socrata" %% "balboa-client-kafka" % "0.16.+",
   ...
 )
```

Others: TODO

## Usage and Code Example

### Metric Logging

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

### Topic Naming

Socrata Kafka clusters are currently configured to **allow** the auto creation of topics within all environments.  This
configuration is within chef git@git.socrata.com:socrata-kafka-cookbook.git.  Please refer to the Kafka Manager for your
respective environment for available topic names.  Please precede all of your topics with your respective team name followed
by a ".". IE. *"which.some.query.topic"*, *"metrics.tenant.v1"*.  This will help control namespace collisions and allow
each team to manage there own topics with less fear of collision.

### Creating your own Kafka producer with your own Message type

Creating content to send
------------------------
1. Create your message type and optional Key Type.  Your message type can be considered the end to end abstract data entity that travels from
 producers to consumers.
2. Create your [KafkaCodec](balboa-common-kafka/src/main/scala/com/socrata/balboa/service/kafka/codec/KafkaCodec.scala)
for your message and key types.
3. Discuss with the Metrics team about getting a [Kafka topic](http://kafka.apache.org/documentation.html#introduction)
provisioned for you.

Creating a producer to send your content
----------------------------------------
1. Identify what the name of your topic.  Please refer to above.
2. Identify the IP address of the desired Kafka brokers.
3. Create your own implementation of [GenericKafkaProducer]() using the Codecs you created earlier.

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
