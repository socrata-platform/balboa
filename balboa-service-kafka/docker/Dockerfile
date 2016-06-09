FROM socrata/java8

# Forward the ZooKeeper and Cassandra Port.
# TODO Expose JMX

### Environment Variables
ENV BALBOA_ROOT /srv/balboa-kafka-consumer
ENV BALBOA_KAFKA_CONSUMER_ARTIFACT balboa-service-kafka-assembly.jar
ENV BALBOA_KAFKA_CONSUMER_CONFIG balboa-kafka.properties
ENV BALBOA_LOG4J log4j.properties

# Default Values
ENV BALBOA_KAFKA_CONSUMER_MEMORY 2048m

WORKDIR $BALBOA_ROOT

ADD ship.d /etc/ship.d

COPY ${BALBOA_KAFKA_CONSUMER_CONFIG}.j2 $BALBOA_ROOT/
COPY $BALBOA_KAFKA_CONSUMER_ARTIFACT $BALBOA_ROOT/
COPY ${BALBOA_LOG4J}.j2 $BALBOA_ROOT/
