#!/bin/bash
TIMEFORMAT='%R %U %S'
PGUSER="aa10733"
PGDATABASE="$1"
PGHOST="localhost"
PGPORT=10000
QUERY="$2"
echo "PG QUERY STARTED"
   { time /usr/bin/psql \
      -U "$PGUSER" \
      -h "$PGHOST" \
      -p "$PGPORT" \
      -d "$PGDATABASE" \
      -c "$QUERY"; } ;

echo "PG QUERY DONE"
