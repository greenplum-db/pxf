#!/usr/bin/env bash

set -eox pipefail

BUILD=${PWD}

# Apache Tomcat Native Library requires:
# 1. OpenSSL version 1.0.2 or higher
# 2. APR version 1.4.0 or higher
# 3. JAVA_HOME to be set

# Check OpenSSL version
echo "Checking OpenSSL version"
openssl version

# Check APR version
echo "Checking APR version"
if command -v rpm; then
  rpm -qa | grep -i apr
elif command -v dpkg; then
  dpkg -l "*apr*"
else
  echo "Unsupported operating system ${TARGET_OS}. Exiting..."
  exit 1
fi

# Sources JAVA_HOME
source ~/.pxfrc
echo "JAVA_HOME=$JAVA_HOME"

cd tomcat-native

# Extract the tarball
tar xzf tomcat-native-*.tar.gz

cd tomcat-native-*/native

# Configure
./configure

# make
make

# Copy the so file to the dist_native directory
cp ./.libs/libtcnative-1.so "${BUILD}/dist_native/"
