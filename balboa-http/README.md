# balboa-http

balboa-http is the service that wraps the metrics persistence store, Cassandra.
It provides a simple, RESTful interface for reading stored metrics.

## Build

balboa-http can built into an executable Assembly (Fat) Jar.

```
sbt balboa-http/assembly
```

### Docker

See [balboa-http/docker/README.md](docker/README.md).

### Configuration

Configuration is done through environmental variables. The list of
configurable variables can be found in each project's reference.conf
file. Defaults can be found in the application.conf files.

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
sbt balboa-http/run
sbt balboa-http/it:test
```

### Load Tests

balboa-http includes a set of load tests using [Gatling](http://gatling.io/#/).
These live in balboa-http/src/it/scala/com.socrata.balboa.server/load.
Each file implements a Gatling simulation designed to load test a
particular aspect of balboa-http. After the tests have been run, the
commandline output will include the URL of a local file where the results
may be viewed.

```
sbt balboa-http/run
sbt gatling-it:test
```

To run the load tests against a specific port or URL, use the environment
variables `SERVICE_HOST` and `SERVICE_PORT`, just as in the normal
integration tests.

##### File Load Test

The `ReplayFileLoadTest` simulation runs a simulation from a file.
It can be run in isolation with:

```
sbt gatling-it:test-only com.socrata.balboa.server.load.ReplayFileLoadTest
```

This simulation looks for the file pointed to by the `replay_load_test_file`
variable in `src/it/resources/reference.conf` or with the 
`REPLAY_LOAD_TEST_FILE` environment variable.

Similarly, the simulation's timeout can be set with the `replay_load_test_timeout_sec`
variable or with the `REPLAY_LOAD_TEST_TIMEOUT_SEC` environment variable,
and the number of maximum simultaneous requests can be set with
`replay_load_test_max_users` or with the `REPLAY_LOAD_TEST_MAX_USERS`
environment variable.

Input files should be newline-delimited sets of requests URIs and parameters
(e.g. `/metrics/oogle/range?foo=bar`).
