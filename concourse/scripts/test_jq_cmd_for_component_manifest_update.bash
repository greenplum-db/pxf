#!/usr/bin/env bash

HERE=$(dirname $0)

INPUT_FILE=/tmp/test-component-manifest.json
OUTPUT_FILE=/tmp/test-component-manifest-output.json
EXPECTED_OUTPUT_FILE=/tmp/test-component-manifest-expected.json

source "$HERE/add_pxf_to_manifest.bash"

echo '### TEST: adding PXF to an empty component list'

cat > "$INPUT_FILE" <<EOF
{
  "platforms": {
    "centos6": {
      "components": [
      ]
    },
    "centos7": {
      "components": [
      ]
    },
    "sles11": {
      "components": [
      ]
    }
  }
}
EOF

cat > "$EXPECTED_OUTPUT_FILE" <<EOF
{
  "platforms": {
    "centos6": {
      "components": [
        {
          "name": "pxf",
          "version": "1.2.3",
          "bucket": "gpdb-stable-concourse-builds",
          "path": "components/pxf",
          "filetype": "tar.gz",
          "artifact_version": "latest"
        }
      ]
    },
    "centos7": {
      "components": [
        {
          "name": "pxf",
          "version": "1.2.3",
          "bucket": "gpdb-stable-concourse-builds",
          "path": "components/pxf",
          "filetype": "tar.gz",
          "artifact_version": "latest"
        }
      ]
    },
    "sles11": {
      "components": []
    }
  }
}
EOF

add_pxf_to_manifest 1.2.3 "$INPUT_FILE" > "$OUTPUT_FILE"
diff -u "$EXPECTED_OUTPUT_FILE" "$OUTPUT_FILE" || exit 1
echo PASS

echo '### TEST: adding PXF leaves existing components in place'

cat > "$INPUT_FILE" <<EOF
{
  "platforms": {
    "centos6": {
      "components": [
        {"name": "foo"}
      ]
    },
    "centos7": {
      "components": [
        {"name": "bar"}
      ]
    },
    "sles11": {
      "components": [
        {"name": "baz"}
      ]
    }
  }
}
EOF

cat > "$EXPECTED_OUTPUT_FILE" <<EOF
{
  "platforms": {
    "centos6": {
      "components": [
        {
          "name": "foo"
        },
        {
          "name": "pxf",
          "version": "1.2.3",
          "bucket": "gpdb-stable-concourse-builds",
          "path": "components/pxf",
          "filetype": "tar.gz",
          "artifact_version": "latest"
        }
      ]
    },
    "centos7": {
      "components": [
        {
          "name": "bar"
        },
        {
          "name": "pxf",
          "version": "1.2.3",
          "bucket": "gpdb-stable-concourse-builds",
          "path": "components/pxf",
          "filetype": "tar.gz",
          "artifact_version": "latest"
        }
      ]
    },
    "sles11": {
      "components": [
        {
          "name": "baz"
        }
      ]
    }
  }
}
EOF

add_pxf_to_manifest 1.2.3 "$INPUT_FILE" > "$OUTPUT_FILE"
diff -u "$EXPECTED_OUTPUT_FILE" "$OUTPUT_FILE" || exit 1
echo PASS

echo '### TEST: adding PXF preserves keys in the root object'

cat > "$INPUT_FILE" <<EOF
{
  "some-key": "value",
  "platforms": {}
}
EOF

cat > "$EXPECTED_OUTPUT_FILE" <<EOF
{
  "some-key": "value",
  "platforms": {}
}
EOF

add_pxf_to_manifest 1.2.3 "$INPUT_FILE" > "$OUTPUT_FILE"
diff -u "$EXPECTED_OUTPUT_FILE" "$OUTPUT_FILE" || exit 1
echo PASS

echo '### TEST: adding PXF preserves keys in the platform objects'

cat > "$INPUT_FILE" <<EOF
{
  "platforms": {
    "centos6": {
      "some-key": "value1",
      "components": []
    },
    "another-platform": {
      "another-key": "value2",
      "components": []
    }
  }
}
EOF

cat > "$EXPECTED_OUTPUT_FILE" <<EOF
{
  "platforms": {
    "centos6": {
      "some-key": "value1",
      "components": [
        {
          "name": "pxf",
          "version": "1.2.3",
          "bucket": "gpdb-stable-concourse-builds",
          "path": "components/pxf",
          "filetype": "tar.gz",
          "artifact_version": "latest"
        }
      ]
    },
    "another-platform": {
      "another-key": "value2",
      "components": []
    }
  }
}
EOF

add_pxf_to_manifest 1.2.3 "$INPUT_FILE" > "$OUTPUT_FILE"
diff -u "$EXPECTED_OUTPUT_FILE" "$OUTPUT_FILE" || exit 1
echo PASS
