#!/usr/bin/env bash

tar -xzf pxf-build-dependencies/pxf-build-dependencies.tar.gz -C ~gpadmin
ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root
chown -R gpadmin:gpadmin ~gpadmin
