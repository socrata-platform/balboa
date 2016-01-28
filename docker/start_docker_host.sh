#!/usr/bin/env bash

# Check for required applications
command -v docker-machine >/dev/null 2>&1 || { echo >&2 "I require docker-machine but it's not installed.  \
    Try `brew install docker-machine`  Aborting."; exit 1; }

# Destroys and Recreates docker host to run urteil.

#if [[ $(docker-machine ls -f "name=balboa") ]]; then
#    echo "2. Restarting old docker host"
#    docker-machine restart balboa
#else
#    echo "2. Creating a new docker host"
#    docker-machine create -d virtualbox --engine-insecure-registry registry.docker.aws-us-west-2-infrastructure.socrata.net:5000 balboa
#fi

docker-machine create -d virtualbox --engine-insecure-registry registry.docker.aws-us-west-2-infrastructure.socrata.net:5000 balboa

echo "3. Update docker environment variables"
echo 'Run eval "$(docker-machine env balboa)"'
