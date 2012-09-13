#!/bin/bash
set -e

cd "$(dirname "$0")"

cmd="$1"
shift

case "$cmd" in
  http)
    mvn install
    cd balboa-http
    exec mvn jetty:run -Djetty.port=9898 "$@"
    ;;
  jms)
    mvn install
    cd balboa-jms
    #exec java -agentpath:/Applications/YourKit_Java_Profiler_9.5.6.app/bin/mac/libyjpagent.jnilib -jar target/balboa-jms-jar-with-dependencies.jar 2 failover:tcp://10.0.2.135:61616 Metrics2
    exec java -jar target/balboa-jms-jar-with-dependencies.jar 2 failover:tcp://localhost:61616 Metrics2
    ;;
  *)
    echo Unknown command "$cmd"
    echo Possible options are: http, jms
esac
