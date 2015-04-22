# Balboa Kafka Service

This project provides an extensible library for subscribing to a Kafka topic and ingest messages from an existing Kafka
Cluster.  This library provides defines a simple interface for creating necessary components for Kafka Message consumption.

## Motivation

Kafka has become a proven distributed messaging bus for real-time data pipelines.  One of the most popular messaging patterns
 is the publish and subscribe pattern.  Kafka has been proven to be a multi-purpose message system that can be utilized
 and managed in a large scale distributed environment.


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
    "com.socrata" % "balboa-kafka-service" %% "0.15.+",
    ...
  )
 ```

Others: TODO

## Usage and Code Example

Terminology
-----------

* appName : An Application name you define.  This allows you to create multiple instances of the same application.  
The appName is a direct association to Kafka Consumer Group.  This guarantees that Kafka will send a single message 
to one of the running applications.
* topic : The Kafka topic to listen for.

Running out of the box
----------------------

`cd balboa && sbt assembly`
`sbt balboa-kafka-service/run "-appName=<YourApplicationName> -topic=<Topic>"`

Creating content to send
------------------------
1. Create your message type and optional Key Type.  Your message type can be considered the end to end abstract data entity that travels from
 producers to consumers.
2. Create your [KafkaCodec](../balboa-common-kafka/src/main/scala/com/socrata/balboa/service/kafka/codec/KafkaCodec
.scala)
for your message and key types.
3. Discuss with the Metrics team about getting a [Kafka topic](http://kafka.apache.org/documentation.html#introduction)
provisioned for you.

Subscribing to messages you define can be summarized in the following steps.
-----------------------------------------------------------------------------
1. Create your own subclass of [KafkaConsumerComponent](TODO)
2. Create your own subclass of [KafkaConsumerGroupComponent](TODO)
3. Create your own subclass of [KafkaConsumerCLIBase](TODO)
4. Point assembly to your subclass of KafkaConsumerCLIBase
`mainClass in assembly := Some("your.subclass.of.KafkaConsumerCLIBase")`

## Tests

Currently there is an issue with testing with SBT via the command line.  Creating a Zookeeper server resolves in address
already in use exception.  We believe this is some how related to tests colliding with eachother.

```
sbt balboa-kafka-service/test
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
