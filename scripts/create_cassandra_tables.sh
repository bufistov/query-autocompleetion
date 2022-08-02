#!/bin/bash -e

docker exec -it autocompletedemo-cassandra cqlsh -f /resources/create_tables.cqlsh
