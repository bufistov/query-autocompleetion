#!/bin/bash

ORG_BUFISTOV_AUTOCOMPLETE_QUERY_HANDLER_LOG_LEVEL=debug \
ORG_BUFISTOV_AUTOCOMPLETE_QUERY_UPDATE_COUNT=1 \
ORG_BUFISTOV_AUTOCOMPLETE_FIRST_QUERY_UPDATE_COUNT=1 \
./gradlew bootRun

