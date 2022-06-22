#!/bin/bash -e

docker exec -it autocompletedemo-cassandra cqlsh -f /app/create_tables.cqlsh
