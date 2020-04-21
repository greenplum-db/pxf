ifeq "$(GPHOME)" ""
    GPHOME = "/usr/local/greenplum-db"
endif

ifeq "$(PXF_HOME)" ""
    PXF_HOME = "$(GPHOME)/pxf"
endif

SHELL := /bin/bash

export GPHOME
export PXF_HOME

PXF_VERSION := $(shell grep version= server/gradle.properties | cut -d= -f2)
export PXF_VERSION

default: all

.PHONY: all external-table cli server install stage tar clean test it help

all: external-table cli server

external-table:
	make -C external-table

cli:
	make -C cli/go/src/pxf-cli

server:
	make -C server

clean:
	rm -rf build
	make -C external-table clean-all
	make -C cli/go/src/pxf-cli clean
	make -C server clean

test:
	make -C cli/go/src/pxf-cli test
	make -C server test

it:
	make -C automation TEST=$(TEST)

install:
	make -C external-table install
	make -C cli/go/src/pxf-cli install
	make -C server install

stage:
	make -C external-table stage
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	GP_MAJOR_VERSION=$$(cat external-table/build/metadata/gp_major_version) ;\
	GP_BUILD_ARCH=$$(cat external-table/build/metadata/build_arch) ;\
	PXF_PACKAGE_NAME=pxf-gpdb$${GP_MAJOR_VERSION}-$${PXF_VERSION}-$${GP_BUILD_ARCH} ;\
	mkdir -p build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a external-table/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a cli/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a server/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	echo $$(git rev-parse --verify HEAD) > build/stage/$${PXF_PACKAGE_NAME}/pxf/commit.sha ;\
	cp package/install_component build/stage/$${PXF_PACKAGE_NAME} ;\

tar: stage
	mkdir -p build/dist
	tar -czf build/dist/$(shell ls build/stage).tar.gz -C build/stage $(shell ls build/stage)

help:
	@echo
	@echo 'Possible targets'
	@echo	'  - all (external-table, cli, server)'
	@echo	'  - external-table - build Greenplum external table extension'
	@echo	'  - cli - install Go CLI dependencies and build Go CLI'
	@echo	'  - server - install PXF server dependencies and build PXF server'
	@echo	'  - clean - clean up external-table, CLI and server binaries'
	@echo	'  - test - runs tests for PXF Go CLI and server'
	@echo	'  - install - install PXF external table extension, CLI and server'
	@echo	'  - tar - bundle PXF external table extension, CLI, server and tomcat into a single tarball'
