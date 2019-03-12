ifeq "$(PXF_HOME)" ""
    ifneq "$(GPHOME)" ""
        PXF_HOME = "$(GPHOME)/pxf"
    endif
endif

SHELL := /bin/bash

export PXF_HOME

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

it:
	make -C automation TEST=$(TEST)

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
