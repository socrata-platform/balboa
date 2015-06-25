FROM socrata/java

# Forward the ZooKeeper and Cassandra Port.
ENV BALBOA_PORT 2012
ENV JMX_PORT 40506
EXPOSE $BALBOA_PORT $JMX_PORT

### Environment Variables
ENV BALBOA_ROOT /srv/balboa-service-jms
ENV BALBOA_ARTIFACT balboa-service-jms-assembly.jar
ENV BALBOA_CONFIG balboa.properties
ENV BALBOA_LOG4J log4j.properties

# Default Values
ENV JAVA_XMX 1024m

WORKDIR $BALBOA_ROOT

ADD ship.d /etc/ship.d

COPY $BALBOA_ARTIFACT $BALBOA_ROOT/
COPY ${BALBOA_CONFIG}.j2 $BALBOA_ROOT/
COPY ${BALBOA_LOG4J}.j2 $BALBOA_ROOT/
