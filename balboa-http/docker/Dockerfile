FROM socrata/runit-java8-bionic

# Forward the ZooKeeper and Cassandra Port.
ENV BALBOA_PORT 2012
EXPOSE $BALBOA_PORT

### Environment Variables
ENV APP balboa-http
ENV SERVICE_DIR /etc/service/${APP}
ENV BALBOA_ARTIFACT ${APP}-assembly.jar
ENV BALBOA_CONFIG balboa.properties
ENV BALBOA_LOG4J log4j.properties

# Default Values
ENV JAVA_XMX 1024m
ENV JAVA_MAX_METASPACE_SIZE 64m

RUN mkdir ${SERVICE_DIR}

COPY $BALBOA_ARTIFACT ${SERVICE_DIR}/
COPY ${BALBOA_CONFIG}.j2 ${SERVICE_DIR}/
COPY ${BALBOA_LOG4J}.j2 ${SERVICE_DIR}/
COPY run ${SERVICE_DIR}/
