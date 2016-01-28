#!/usr/bin/env bash

# Docker Compose based Balboa agent creation script.
command -v docker-compose >/dev/null 2>&1 || { echo >&2 "I require docker-compose but it's not installed.  Aborting."; exit 1; }

# The directory of this file for relative path funzies.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Starting docker host with ${DIR}/../docker/start_docker_host.sh"
${DIR}/../docker/start_docker_host.sh

# Set up stateful services.
docker-compose -f ${DIR}/01-compose.yml up -d
