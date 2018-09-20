#!/bin/bash

mygphome=${GPHOME:-../../product/greenplum-db}
source ${mygphome}/greenplum_path.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
EXT=$DIR/../ext

EPYDOC_DIR=epydoc-3.0.1
EPYDOC_PYTHONPATH=${EXT}:${EXT}/${EPYDOC_DIR}/build/lib/

pushd $DIR/..

source tinc_env.sh

make epydocs
PYTHONPATH=${PYTHONPATH}:${EPYDOC_PYTHONPATH} python ${EXT}/${EPYDOC_DIR}/build/scripts-*/epydoc -v --graph umlclasstree --no-private --config=.epydoc.config

if [ "x${BLDWRAP_TOP}" != "x" ]; then
    ssh build@build-prod.sanmateo.greenplum.com "rm -rf /var/www/html/QA/reports/tinc/epydocs/${PULSE_PROJECT}/*"
    ssh build@build-prod.sanmateo.greenplum.com "mkdir -p /var/www/html/QA/reports/tinc/epydocs/${PULSE_PROJECT}"
    scp -r epydocs/* build@build-prod.sanmateo.greenplum.com:/var/www/html/QA/reports/tinc/epydocs/${PULSE_PROJECT}/
fi

# now if tincrepo is there, make epydocs for that also
if [ "x${TINCREPOHOME}" != "x" ]; then
    pushd ${TINCREPOHOME}
    source tincrepo_env.sh
    PYTHONPATH=${PYTHONPATH}:${EPYDOC_PYTHONPATH} python ${EXT}/${EPYDOC_DIR}/build/scripts-*/epydoc -v --graph umlclasstree --no-private --config=.epydoc.config

    if [ "x${BLDWRAP_TOP}" != "x" ]; then
        ssh build@build-prod.sanmateo.greenplum.com "rm -rf /var/www/html/QA/reports/tincrepo/epydocs/${PULSE_PROJECT}/*"
        ssh build@build-prod.sanmateo.greenplum.com "mkdir -p /var/www/html/QA/reports/tincrepo/epydocs/${PULSE_PROJECT}"
        scp -r epydocs/* build@build-prod.sanmateo.greenplum.com:/var/www/html/QA/reports/tincrepo/epydocs/${PULSE_PROJECT}/
    fi
    popd
fi


popd
