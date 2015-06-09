FROM socrata/java

### Environment Variables
ENV BALBOA_ROOT /srv/balboa-tmp
ENV BALBOA_TMP_ARTIFACT balboa-tmp-assembly.jar
ENV BALBOA_TMP_CONFIG balboa.properties
ENV BALBOA_TMP_LOG4J log4j.properties

WORKDIR $BALBOA_ROOT

ADD ship.d /etc/ship.d


COPY ${BALBOA_TMP_CONFIG}.j2 $BALBOA_ROOT/
COPY $BALBOA_TMP_LOG4J $BALBOA_ROOT/
COPY $BALBOA_TMP_ARTIFACT $BALBOA_ROOT/
