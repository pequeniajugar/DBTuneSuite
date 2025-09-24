#!/bin/bash
# === base.sh ===
# Utility script for PostgreSQL server control and max_connections adjustment

set -euo pipefail

PGUSER="postgres"   # username
PGPORT=10001       # default port
PGHOST="localhost"  # default host

# Function to get PostgreSQL data directory (PGDATA)
get_pgdata() {
  psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -At -c "SHOW data_directory;" | tr -d '[:space:]'
}

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <command> [args]"
  echo "Commands:"
  echo "  start                 Start PostgreSQL"
  echo "  stop                  Stop PostgreSQL"
  echo "  restart               Restart PostgreSQL"
  echo "  set-maxconn <number>  Set max_connections and restart PostgreSQL"
  exit 1
fi

CMD="$1"
shift

case "$CMD" in
  start)
    PGDATA=$(get_pgdata)
    echo "Starting PostgreSQL (PGDATA=$PGDATA)..."
    pg_ctl -D "$PGDATA" -o "-p $PGPORT" -l logfile start
    ;;
  stop)
    PGDATA=$(get_pgdata)
    echo "Stopping PostgreSQL (PGDATA=$PGDATA)..."
    pg_ctl -D "$PGDATA" stop -m fast
    ;;
  restart)
    PGDATA=$(get_pgdata)
    echo "Restarting PostgreSQL (PGDATA=$PGDATA)..."
    pg_ctl -D "$PGDATA" restart -m fast
    ;;
  set-maxconn)
    if [[ $# -lt 1 ]]; then
      echo "Usage: $0 set-maxconn <number>"
      exit 1
    fi
    NEWVAL="$1"
    echo "Setting max_connections = $NEWVAL"
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres -c "ALTER SYSTEM SET max_connections = '${NEWVAL}';"
    PGDATA=$(get_pgdata)
    echo "Restarting PostgreSQL (PGDATA=$PGDATA) to apply change..."
    pg_ctl -D "$PGDATA" restart -m fast
    ;;
  *)
    echo "Unknown command: $CMD"
    exit 1
    ;;
esac
