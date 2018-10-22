#!/usr/bin/env bash

HERE="$(dirname "$0")"
OUTPUT=$(mktemp /tmp/test-output.XXXXXX)

function fail()
{
    echo "FAILURE. Output was:"
    cat "$OUTPUT"
    exit 1
}

echo 'TEST: update-config with no arguments'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config

grep -q 'update-config requires arguments' "$OUTPUT" || fail
grep -q 'Usage: ' "$OUTPUT" || fail
echo 'PASS'

echo 'TEST: update-config with an unrecognized option'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -x

grep -q 'Unrecognized option: -x' "$OUTPUT" || fail
grep -q 'Usage: ' "$OUTPUT" || fail
echo 'PASS'

echo 'TEST: update-config -f with no argument'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f

grep -q 'Invalid option: -f requires an argument' "$OUTPUT" || fail
echo 'PASS'

echo 'TEST: update-config with extra arguments'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f file -x extra

grep -q 'Unrecognized option: -x' "$OUTPUT" || fail
echo 'PASS'

echo 'TEST: update-config with a nonexistent hostfile'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f /tmp/nonexistent

grep -q 'Invalid option: /tmp/nonexistent is not a file' "$OUTPUT" || fail
echo 'PASS'

rm /tmp/test-output.*