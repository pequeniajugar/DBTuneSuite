#!/bin/bash
TIMEFORMAT='%R %U %S'
MYSQL_USER="root"
MYSQL_PASSWORD="pwd"
MYSQL_HOST="localhost"
MYSQL_PORT=3306
MYSQL_DATABASE="$1"
QUERY="$2"
echo "MYSQL QUERY STARTED"
       /data/aa10733/mysql/bin/mysql \
      -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" \
      -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DATABASE" -e "$QUERY"

      if [ $? -ne 0 ]; then
        echo "Error: Connection to MySQL server failed."
        echo "MySQL Error Details:"
        exit 1
     fi
echo "MYSQL QUERY DONE"
