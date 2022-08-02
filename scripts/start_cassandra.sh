#!/bin/bash

container_name=autocompletedemo-cassandra
isrunning=$(docker ps --filter name=${container_name} -q)
if [[ -n "${isrunning}" ]]
then
  echo $isrunning
else
  docker run --name  ${container_name} -v "$(pwd)/data:/var/lib/cassandra" -v "$(pwd)/src/main/resources:/resources"  -p 9042:9042 -d cassandra:latest
fi
