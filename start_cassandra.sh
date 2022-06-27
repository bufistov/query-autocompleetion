#!/bin/bash

container_name=autocompletedemo-cassandra
isrunning=$(docker ps --filter name=${container_name} -aq)
if [[ -n "${isrunning}" ]]
then
  echo $isrunning
else
  docker run --name  ${container_name} -v "$(pwd)/data:/var/lib/cassandra"  -v "$(pwd):/app" -p 9042:9042 -d cassandra:latest
fi