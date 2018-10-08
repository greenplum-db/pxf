#!/bin/bash

pushd ~/workspace/gpdb
make -j4 install
popd

# Install the 'psi' library in GPDB's libs.
# See: https://pypi.org/project/PSI/
sudo cp -r $(find /usr/lib64 -name psi | sort -r | head -1) ${GPHOME}/lib/python