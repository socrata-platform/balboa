# Balboa

**Balboa** is Socrata's internal tenant metrics system. This project is
composed of different types of subprojects. Together they handle getting
metrics from internal Socrata services, into the metrics database, and then
answer questions about the stored metrics.

Roughly speaking the metrics intake pipeline is:

    [Socrata Service]
     |
    proprietary file on disk
     |
    balboa-agent (one per service host)
     |
    JMS
     |
    balboa-service-jms
     |
    Cassandra

Roughly speaking, the metrics serving pipeline is:

    balboa-http
     |
    Cassandra

### Core

TODO: Complete README refactor

### Common

Shared libraries, like configuration and message format data structures.

### HTTP

balboa-http is an http service that accepts queries for aggregated metrics
values. It is the read portion of the balboa metrics system.

### Clients

Client libraries provide a mechanism to publish metrics to an existing
messaging bus.

* [Balboa JMS Client] TODO: Complete README refactor

### Services

These services are part of the metrics intake system. balboa-service-jms
listens on a JMS queue for metrics written by balboa-agent and saves those
metrics to the database.

* [Balboa JMS Service] TODO: Complete README refactor

## Usage

### REST API
#### Range Queries
```
GET /metrics/{entity}/range?
   start={YYYY-MM-DD HH:MM:SS:mmmm}&
   end={YYYY-MM-DD HH:MM:SS:mmmm}
```

200: Returns JSON of the form:

```
{
  {metric name 1}: {
    value: {aggregate value for range},
    type: "{aggregate|absolute}"
  },
  {metric name 2} {... }
  ...
  {metric name n} {... }
}
```

A metric name is a particular, tracked metric within an entity. Metrics can
be either aggregate metrics (accumulated within a period) or absolute (replaced
within a period).

You can also query for multiple entities at the same time.

```
GET /metrics/range?entityId={entity1}&entityId={entity2}&
   start={YYYY-MM-DD HH:MM:SS:mmmm}&
   end={YYYY-MM-DD HH:MM:SS:mmmm}
```

200: Returns JSON of the form:

```
{
  {entity1}: {
    {metric name 1}: {
      value: {aggregate value for range},
      type: "{aggregate|absolute}"
    },
    {metric name 2} {... }
    ...
    {metric name n} {... }
  },
  {entity2}: { ... }
}
```

#### Series Queries
```
GET /metrics/{entity}/series?
   start={YYYY-MM-DD HH:MM:SS:mmmm}&
   end={YYYY-MM-DD HH:MM:SS:mmmm}&
   period={YEARLY|MONTHLY|DAILY|HOURLY|FIFTEEN_MINUTE}
```

200: Returns JSON of the form:

```
[
   {
      start: {time0:start},
      end: {time1},
      metrics: { }
   },
   {
      start: {time1},
      end: {time2},
      metrics: { }
   },
   ...
   {
      start: {time n-1},
      end: {time n:end},
      metrics: { }
   }
]
```

Again, it's possible to query for multiple entities at the same time.

```
GET /metrics/series?entityId={entity1}&entityId={entity2}&
   start={YYYY-MM-DD HH:MM:SS:mmmm}&
   end={YYYY-MM-DD HH:MM:SS:mmmm}&
   period={YEARLY|MONTHLY|DAILY|HOURLY|FIFTEEN_MINUTE}
```

200: Returns JSON of the form:

```
{
  {entity1}: [
    {
      start: {time0:start},
      end: {time1},
      metrics: { }
    },
    {
      start: {time1},
      end: {time2},
      metrics: { }
    },
    ...
    {
      start: {time n-1},
      end: {time n:end},
      metrics: { }
    }
  ],
  {entity2}: [
    ...
  ]
}
```

### Client
**Balboa-client** is the library which allows individual services to start the
process of sending metrics to the metrics store. There are two supported
options for sending metrics:

1. Use MetricFileQueue to write the metrics to a well known directory on disk
   where a locally running balboa-agent finds the files and puts the metrics
   onto the JMS queue.
2. Use MetricJmsQueue which writes the metrics directly to the JMS queue.

To include it in a project:

1. Add the Socrata release repository to the project's `build.sbt`:
```
resolvers := Seq(
  ...
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  ...
)
```
2. Add the balboa-client dependency to the project's library dependencies:
```
libraryDependencies ++= Seq(
  ...
  "com.socrata" %% "balboa-client" % "0.17.6",
  ...
)
```

### Server
**Balboa server** requires four separate components to function properly:

* [ActiveMQ](http://activemq.apache.org) (v5.5.1)
* A [Cassandra](http://cassandra.apache.org) cluster (v1.1.7)
* balboa-jms
* balboa-http

#### Setup (balboa-jms and balboa-http)

##### Configuration
Balboa has default configuration files for each runnable project, under
`[project]/src/main/resources/application.conf`.  To override any of
these settings, set the corresponding environmental variable found in
`[project]/src/main/resources/reference.conf`.

##### Building and Running

1. From the project root, run `sbt assembly`. This will produce two standalone assembly jars:
`balboa-jms/target/scala-2.10/balboa-jms-assembly-[VERSION].jar` and `balboa-http/target/scala-2.10/balboa-http-assembly-[VERSION].jar`, where VERSION is the current version of the project as defined in `version.sbt`.

2. Assuming Cassandra and ActiveMQ are installed, create the Cassandra schema:
`cassandra-cli < etc/balboa-cassandra.schema`. By default, this creates the `Metrics2012` keyspace. If a different keyspace is desired, tweak the schema file as necessary.

3. Start balboa-jms with `java -jar [JAR] [NUM_THREADS] [JMS_SERVER] [KEYSPACE]`. In a typical developer setup, this might look something like `java -jar balboa-jms/target/scala-2.10/balboa-jms-assembly-[VERSION].jar 1 failover:tcp://localhost:61616 Metrics2012`.

4. Start balboa-http with `java -jar [JAR] [PORT]`. In a typical developer setup, this might look something like `java -jar balboa-http/target/scala-2.10/balboa-http-assembly-[VERSION].jar 2012`.

At this point, balboa-client should be capable of depositing metrics into
balboa server, which can then be queried through the REST API provided by
balboa-http.

##### Admin Tool

BalboaAdmin is a tool for dumping and loading metrics. It is extemely useful for correcting metrics which have been incorrectly set; or for migrating metrics across environments.

```
> java -cp balboa-admin-assembly-0.14.1-SNAPSHOT.jar com.socrata.balboa.admin.BalboaAdmin
Balboa admin utility:
       java -jar balboa-admin <command> [args]

Commands:
	fsck [filters...]  : Check the balboa file system and validate the correctness of the tiers. This will probably take a long time.
	fill file          : Restore balboa metrics from [file].
	dump [filters...]  : Dump all of the data in a balboa store to stdout in a format suitable for fill, with an optional entity regex
	dump-only entityId : Dump a specific entity in a format suitable for fill
	list [filters...]  : Dump all of the entity keys in a balboa store to stdout, with an optional entity regex
```

###### Example: Dump Metrics for Entity "foo"

```
java -cp balboa-admin-assembly-0.14.1-SNAPSHOT.jar com.socrata.balboa.admin.BalboaAdmin dump foo
"foo","1414875600000","datasets","absolute","387"
"foo","1414879200000","datasets","absolute","387"
"foo","1414882800000","datasets","absolute","387"
"foo","1414886400000","datasets","absolute","387"
...
```
####### Example: Fill Metrics for Entity "foo"

```
java -cp balboa-admin-assembly-0.14.1-SNAPSHOT.jar com.socrata.balboa.admin.BalboaAdmin fill dataset.counts.out.fixed
Reading from file dataset.counts.out.fixed
INFO - Persisted entity: foo with 1 absolute and 0 aggregated metrics - took 73ms
INFO - Persisted entity: foo with 1 absolute and 0 aggregated metrics - took 15ms
INFO - Persisted entity: foo with 1 absolute and 0 aggregated metrics - took 14ms
```

## Releases

As of 0.13.0, balboa is versioned with [Semantic Versioning](http://www.semver.org), and uses the [sbt-release](https://github.com/sbt/sbt-release) plugin. To release balboa, run `sbt release` from the project root.

## Development

There is some unique configuration around cross compiling. See comments in the
build.sbt for more details.

## License
Balboa is licensed under the [Apache 2.0](https://github.com/socrata/balboa/blob/master/LICENSE.md) license.
