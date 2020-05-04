#!/usr/bin/env bash

tar -xzf /tmp/build/pxf-build-dependencies.tar.gz -C ~gpadmin
rm -rf /tmp/build
ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root
chown -R gpadmin:gpadmin ~gpadmin
