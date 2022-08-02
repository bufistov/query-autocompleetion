#!/bin/bash -e

docker exec -it autocompletedemo-cassandra cqlsh -f /app/src/main/resources/create_tables.cqlsh
