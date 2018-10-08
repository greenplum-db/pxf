#!/bin/bash

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

${CWDIR}/build_gpdb.bash
${CWDIR}/install_gpdb.bash
