#!/usr/bin/env bash

set -x

CASSANDRA_PORT=$(docker inspect --format '{{ (index (index .NetworkSettings.Ports "9042/tcp") 0).HostPort }}' docker_cassandra_1)
ACTIVEMQ_PORT=$(docker inspect --format '{{ (index (index .NetworkSettings.Ports "61616/tcp") 0).HostPort }}' docker_activemq_1)

export BALBOA_SERVICE_JMS_AMQ_SERVER="failover:tcp://127.0.0.1:${ACTIVEMQ_PORT}"
export CASSANDRA_SERVERS="localhost:${CASSANDRA_PORT}"
export BALBOA_SERVICE_JMS_AMQ_USER="admin"
export BALBOA_SERVICE_JMS_AMQ_PASSWORD="admin"
cd .. && sbt 'balboa-service-jms/run'
