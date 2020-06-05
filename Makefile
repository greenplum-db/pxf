include common.mk

PXF_VERSION ?= $(shell cat version)
export PXF_VERSION

LICENSE ?= ASL 2.0
VENDOR ?= Open Source

default: all

.PHONY: all external-table cli server install stage tar rpm rpm-tar clean test it help

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

install-dev:
	make -C cli/go/src/pxf-cli install
	make -C server install-dev

stage:
	rm -rf build/stage
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
	cp package/install_binary build/stage/$${PXF_PACKAGE_NAME}/install_component

tar: stage
	rm -rf build/dist
	mkdir -p build/dist
	tar -czf build/dist/$(shell ls build/stage).tar.gz -C build/stage $(shell ls build/stage)

rpm:
	make -C external-table stage
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	GP_MAJOR_VERSION=$$(cat external-table/build/metadata/gp_major_version) ;\
	PXF_MAIN_VERSION=$${PXF_VERSION//-SNAPSHOT/} ;\
	if [[ $${PXF_VERSION} == *"-SNAPSHOT" ]]; then PXF_RELEASE=SNAPSHOT; else PXF_RELEASE=1; fi ;\
	rm -rf build/rpmbuild ;\
	mkdir -p build/rpmbuild/{BUILD,RPMS,SOURCES,SPECS} ;\
	mkdir -p build/rpmbuild/SOURCES/gpextable ;\
	cp -a external-table/build/stage/* build/rpmbuild/SOURCES/gpextable ;\
	cp -a cli/build/stage/pxf/* build/rpmbuild/SOURCES ;\
	cp -a server/build/stage/pxf/* build/rpmbuild/SOURCES ;\
	echo $$(git rev-parse --verify HEAD) > build/rpmbuild/SOURCES/commit.sha ;\
	cp package/*.spec build/rpmbuild/SPECS/ ;\
	rpmbuild \
	--define "_topdir $${PWD}/build/rpmbuild" \
	--define "pxf_version $${PXF_MAIN_VERSION}" \
	--define "pxf_release $${PXF_RELEASE}" \
	--define "license ${LICENSE}" \
	--define "vendor ${VENDOR}" \
	-bb $${PWD}/build/rpmbuild/SPECS/pxf-gp$${GP_MAJOR_VERSION}.spec

rpm-tar: rpm
	rm -rf build/{stagerpm,distrpm}
	mkdir -p build/{stagerpm,distrpm}
	set -e ;\
	GP_MAJOR_VERSION=$$(cat external-table/build/metadata/gp_major_version) ;\
	PXF_RPM_FILE=$$(find build/rpmbuild/RPMS -name pxf-gp$${GP_MAJOR_VERSION}-*.rpm) ;\
	PXF_RPM_BASE_NAME=$$(basename $${PXF_RPM_FILE%*.rpm}) ;\
	PXF_PACKAGE_NAME=$${PXF_RPM_BASE_NAME%.*} ;\
	mkdir -p build/stagerpm/$${PXF_PACKAGE_NAME} ;\
	cp $${PXF_RPM_FILE} build/stagerpm/$${PXF_PACKAGE_NAME} ;\
	cp package/install_rpm build/stagerpm/$${PXF_PACKAGE_NAME}/install_component ;\
	tar -czf build/distrpm/$${PXF_PACKAGE_NAME}.tar.gz -C build/stagerpm $${PXF_PACKAGE_NAME}

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
	@echo	'  - install-dev - install CLI and server without running tests'
	@echo	'  - tar - bundle PXF external table extension, CLI, server and tomcat into a single tarball'
	@echo	'  - rpm - create PXF RPM / DEB package'
	@echo	'  - rpm-tar - bundle PXF RPM / DEB package along with helper scripts into a single tarball'
