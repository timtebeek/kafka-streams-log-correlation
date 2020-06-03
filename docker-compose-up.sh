#!/bin/bash
set -e;
clear;
clear; 

# Start containers
docker-compose up \
 --abort-on-container-exit \
 --remove-orphans ;

# Remove containers and anonymous volumes
docker-compose rm --force --stop -v ;
docker volume prune;

