#!/bin/bash -e

docker exec -it autocompletedemo-cassandra cqlsh --request-timeout 60 -f /app/src/main/resources/drop_tables.cqlsh
