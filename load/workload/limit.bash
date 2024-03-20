#!/usr/bin/env bash

set -eo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

source "${SCRIPT_DIR}/common.bash"

# The job cannot reliably succeed in CI environment as we're running into exceeding PXF capacity with about 30 clients.
# This is suspicious, since if only 30 clients work simultaneously, then there are only 30*3=90 concurrent connections
# and Tomcat and async pool is configured for 200 threads.
#
# So we will have this job run only in DEV environment for now and it will likely fail even before attempting to
# run with 100 clients.
#
# What is important, however, is that we should not see "partial file transfer" errors. But I could not see them
# with the old version of Spring MVC either using this test.

run_pgbench "limit_query" 10 1000

run_pgbench "limit_query" 20 1000

run_pgbench "limit_query" 30 1000

run_pgbench "limit_query" 40 1000

run_pgbench "limit_query" 50 1000

run_pgbench "limit_query" 60 1000

run_pgbench "limit_query" 100 1000
