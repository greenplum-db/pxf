#!/bin/bash

source /opt/gcc_env.sh # set the compiler to gcc6.2, for C++11 support
cd /home/gpadmin/gpdb
make clean
./configure \
  --enable-debug \
  --with-perl \
  --with-python \
  --with-libxml \
  --disable-orca \
  --prefix=/usr/local/gpdb
# TODO: Change prefix to greenplum-db-devel, as that directory already exists on the container
make -j8
make install

# Install the 'psi' library in GPDB's libs.
# See: https://pypi.org/project/PSI/
sudo cp -r $(find /usr/lib64 -name psi | sort -r | head -1) ${GPHOME}/lib/python

