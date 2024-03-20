#!/usr/bin/env bash

set -eo pipefail

: "${GPHOME:?GPHOME must be set}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# 60 default clients for 3-segment cluster (180 requests to PXF) and pool size of 200 threads
# will likely fail on MacOS due to a limit on the number of ephemeral ports
DEFAULT_CLIENTS=60
# default number of repeated queries to perform
DEFAULT_REPEATS=1500

export PGDATABASE="pxfload"

function run_pgbench() {
  local QUERY=${1}
  local CLIENTS=${2:-${DEFAULT_CLIENTS}}
  local REPEATS=${3:-${DEFAULT_REPEATS}}

  if [[ -r ${QUERY} ]]; then
    echo "File ${QUERY} is not found, exiting"
    exit 1
  fi

  # run pgbench with a given concurrency (clients) and a number of consecutive queries (repeats)
  echo "Running pgbench query '${QUERY}' with ${CLIENTS} clients and ${REPEATS} repeats"
  pgbench -c "${CLIENTS}" -j "${CLIENTS}" -t "${REPEATS}" -f "${SCRIPT_DIR}/../sql/${QUERY}.sql" -n
}
