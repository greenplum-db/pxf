#!/usr/bin/env bash

HERE="$(dirname "$0")"
OUTPUT=$(mktemp /tmp/test-output.XXXXXX)

function fail()
{
    echo -e "\e[91mFAILURE\e[0m. Output was:"
    cat "$OUTPUT"
    exit 1
}

# =========================================================

echo 'TEST: update-config with no arguments'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config

grep -q 'update-config requires arguments' "$OUTPUT" || fail
grep -q 'Usage: pxf update-config' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: update-config with an unrecognized option'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -x

grep -q 'Unrecognized option: -x' "$OUTPUT" || fail
grep -q 'Usage: ' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: update-config -f with no argument'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f

grep -q 'Invalid option: -f requires an argument' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: update-config with extra arguments'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f file -x extra

grep -q 'Unrecognized option: -x' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: update-config with a nonexistent hostfile'

2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f /tmp/nonexistent

grep -q 'Invalid option: /tmp/nonexistent is not a file' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: update-config with blank PXF_USER_HOME'

touch /tmp/test-hostfile
PXF_USER_HOME='' 2>"$OUTPUT" bash "$HERE/pxf-service" update-config -f /tmp/test-hostfile

grep -q 'ERROR: $PXF_USER_HOME is blank or not set' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: start-all with no arguments'

2>"$OUTPUT" bash "$HERE/pxf-service" start-all

grep -q 'start-all requires arguments' "$OUTPUT" || fail
grep -q 'Usage: pxf start-all -f hostfile.txt' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: start-all with an unrecognized option'

2>"$OUTPUT" bash "$HERE/pxf-service" start-all -x

grep -q 'Unrecognized option: -x' "$OUTPUT" || fail
grep -q 'Usage: pxf start-all' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: start-all -f with no argument'

2>"$OUTPUT" bash "$HERE/pxf-service" start-all -f

grep -q 'Invalid option: -f requires an argument' "$OUTPUT" || fail
grep -q 'Usage: pxf start-all' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: start-all -f with extra arguments'

2>"$OUTPUT" bash "$HERE/pxf-service" start-all -f hostfile -x

grep -q 'Unrecognized option: -x' "$OUTPUT" || fail
grep -q 'Usage: pxf start-all' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: start-all with a nonexistent hostfile'

2>"$OUTPUT" bash "$HERE/pxf-service" start-all -f /tmp/nonexistent

grep -q 'Invalid option: /tmp/nonexistent is not a file' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

echo 'TEST: start-all with blank GPHOME'

touch /tmp/test-hostfile
GPHOME='' 2>"$OUTPUT" bash "$HERE/pxf-service" start-all -f /tmp/test-hostfile

grep -q 'ERROR: $GPHOME is blank or not set' "$OUTPUT" || fail
echo 'PASS'

# =========================================================

rm /tmp/test-output.*
rm /tmp/test-hostfile
