#!/bin/bash
set -e

pxf stop || true
make install -C ~/workspace/pxf
rm -rf $PXF_HOME/pxf-service
yes | pxf init
pxf start
