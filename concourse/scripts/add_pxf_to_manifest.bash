#!/usr/bin/env bash

set -o pipefail

function add_pxf_to_manifest() {
    local version="$1"
    local manifest="$2"
    local filterfile=$(mktemp /tmp/jq_filter.XXXXXX)

    cat > "$filterfile" <<EOF

            # Declare some variables
            . as \$root
            | .platforms as \$platforms

            # Get the name of each platform
            | .platforms | keys

            # At this point, the value in the pipeline looks like ["centos6", "centos7", "sles11"].
            # We want to package PXF only with centos 6 and 7, so select only those platforms.
            | map(select(. == "centos6" or . == "centos7"))

            | reduce .[] as \$platform (

                # Starting from the original \$platforms object...
                \$platforms;

                # Build up a new object by deeply merging with objects that have PXF in their
                # components lists.
                . + {
                    (\$platform): (\$platforms."\\(\$platform)" + {
                        components: (\$platforms."\\(\$platform)".components +
                            [{
                                "name":"pxf",
                                "version":"${version}",
                                "bucket":"gpdb-stable-concourse-builds",
                                "path":"components/pxf",
                                "filetype":"tar.gz",
                                "artifact_version":"latest"
                            }]
                        )
                    })
                }
            )

            # Merge the new platforms object into the old \$root.
            | \$root + {"platforms": . }
EOF

    cat "$manifest" | jq -f "$filterfile" || return 1
    rm -f "$filterfile" >/dev/null
}
