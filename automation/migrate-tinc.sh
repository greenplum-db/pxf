#!/usr/bin/env bash

tinc_test_name="$1"

fail() {
    echo >&2 "ERROR: $*"
}

if ! type rename &>/dev/null; then
    faile "rename is required (install with 'brew install rename' or 'apt install util-linux')"
fi

if [[ ! -d $tinc_test_name ]]; then
    fail "tinc test scenarion does not exist"
fi

if [[ ! -d $tinc_test_name/expected ]] || [[ ! -d $tinc_test_name/sql ]] || [[ ! -f $tinc_test_name/__init__.py ]] || [[ ! -f $tinc_test_name/runTest.py ]]; then
    fail "unexpected directory structure"
fi

# convert tinc path to pg_regress path
# example:
#
#   tincrepo/main/pxf/smoke/small_data => smoke/small_data
pg_regress_test_name="regress/${tinc_test_name##*pxf/}"

mkdir -p "${pg_regress_test_name}"
mkdir "${pg_regress_test_name}"/sql
mkdir "${pg_regress_test_name}"/expected

# copy SQL queries and fix-up permissions
cp -a "$tinc_test_name"/sql/*.sql "${pg_regress_test_name}"/sql/
chmod 0644 "${pg_regress_test_name}"/sql/*.sql

# copy expected, changing *.ans to *.out queries and fix-up permissions
# pg_regress looks for *.out, tinc for *.ans
cp -a "$tinc_test_name"/expected/*.ans "${pg_regress_test_name}"/expected/
chmod 0644 "${pg_regress_test_name}"/expected/*.ans
rename --subst ans out "${pg_regress_test_name}"/expected/*.ans
