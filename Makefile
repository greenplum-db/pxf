PXF_VERSION := $(shell grep '^version=' ./server/gradle.properties | cut -d "=" -f2)

ifneq "$(PXF_HOME)" ""
	BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"
else ifneq "$(GPHOME)" ""
	PXF_HOME= "$(GPHOME)/pxf"
	BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"
endif

export PXF_HOME PXF_VERSION BUILD_PARAMS

default: all

.PHONY: all cli server install tar clean test help

all: cli server

cli:
	make -C cli/go/src/pxf-cli

server:
	make -C server

clean:
	make -C cli/go/src/pxf-cli clean
	make -C server clean

test:
	make -C cli/go/src/pxf-cli test
	make -C server test

install:
	make -C cli/go/src/pxf-cli install
	make -C server install

tar:
	make -C cli/go/src/pxf-cli tar
	make -C server tar

help:
	@echo
	@echo 'Possible targets'
	@echo	'  - all (cli, server)'
	@echo	'  - cli - install Go CLI dependencies and build Go CLI'
	@echo	'  - server - install PXF server dependencies and build PXF server'
	@echo	'  - clean - clean up CLI and server binaries'
	@echo	'  - test - runs tests for PXF Go CLI and server'
	@echo	'  - install - install PXF CLI and server'
	@echo	'  - tar - bundle PXF CLI along with tomcat into a single tarball'
