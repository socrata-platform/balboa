FROM socrata/java

### Environment Variables
ENV BALBOA_ROOT /srv/balboa-ticker-tape
ENV BALBOA_ARTIFACT balboa-ticker-tape-assembly.jar
ENV BALBOA_CONFIG balboa.properties
ENV BALBOA_LOG4J log4j.properties

RUN mkdir -p /srv/balboa-ticker-tape
RUN mkdir -p /data/metrics

WORKDIR $BALBOA_ROOT


ADD ship.d /etc/ship.d

COPY ${BALBOA_CONFIG}.j2 $BALBOA_ROOT/
COPY $BALBOA_LOG4J $BALBOA_ROOT/
COPY $BALBOA_ARTIFACT $BALBOA_ROOT/
