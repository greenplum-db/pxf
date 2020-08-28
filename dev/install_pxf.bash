#!/usr/bin/env bash

function display() {
    echo
    echo "=====> $1 <====="
    echo
}

display "Compiling and Installing PXF"
make -C ~gpadmin/workspace/pxf install

display "Initializing PXF"
pxf init

display "Starting PXF"
pxf start

display "Setting up default PXF server"
cp "${PXF_RUN}"/templates/*-site.xml "${PXF_RUN}"/servers/default

display "Registering PXF Greenplum extension"
psql -d template1 -c "create extension pxf"

#cd ~/workspace/pxf/automation
#make GROUP=smoke