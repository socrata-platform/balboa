FROM cassandra:2.2.10

RUN mkdir /docker-entrypoint-initdb.d
COPY balboa_schema.cql /docker-entrypoint-initdb.d/
COPY docker-entrypoint.sh /docker-entrypoint.sh
