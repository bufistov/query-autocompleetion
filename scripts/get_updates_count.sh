#!/bin/bash

grep  'updates finished' "$@" | cut -d\  -f7 | awk '{ sum += $1; n++ } END { if (n > 0) print sum / n; print sum; }'
