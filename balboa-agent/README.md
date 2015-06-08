# Balboa Agent

Is a core process that serves as a consumer and producer of Balboa metrics.  The agent is responsible for reliably consuming messages
through the consumer model and emitting them through a producer.  Producers and Consumers are managed through Balboa Configuration.

## Motivation

Balboa Agents are meant to serve as a standalone process that is meant to reliably transport metrics from a source and emit
 them to another location.  This type of Piping is completely necessary for a system that relies on piping event based data
 for one place to another.

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
    "com.socrata" % "balboa-agent" %% "0.17.+",
    ...
  )
 ```

Others: TODO

## Usage and Code Example

### Terminology

* _agent_ : Standalone process that both consumes and emits metrics.
* _amq_ : Active MQ

### Configuration

Configuration is done through Balboa Configuration properties or via the command line.  Command line arguments overwrite
any found file based configuration properties.  Example configuration can be found in config/config.properties.

### Running out of the box

`cd balboa && sbt assembly`
`sbt balboa-agent/run "Command line arguments"

## Tests

Standard SBT Tests.

```
sbt balboa-agent/test
```

## Contributing

Let people know how they can dive into the project, include important links to things like issue trackers, irc, twitter accounts if applicable.

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request :D

## History

Balboa-agent formerly the Metric Consumer started off as one off project to help ingest and consolidate metrics on individual servers.
This helped manage the amount of traffic being pushed through activemq.

## Credits

TODO: Write credits

## License

See root project license.
