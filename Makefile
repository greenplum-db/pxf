include common.mk

PG_CONFIG = pg_config

PXF_VERSION ?= $(shell cat version)
export PXF_VERSION

FDW_SUPPORT = $(shell $(PG_CONFIG) --version | egrep "PostgreSQL 12")

SOURCE_EXTENSION_DIR = external-table
TARGET_EXTENSION_DIR = gpextable
ifneq ($(FDW_SUPPORT),)
	SOURCE_EXTENSION_DIR = fdw
	TARGET_EXTENSION_DIR = fdw
endif


LICENSE ?= ASL 2.0
VENDOR ?= Open Source

default: all

.PHONY: all external-table fdw cli server install install-server stage tar rpm rpm-tar deb deb-tar clean test it help

all: external-table cli server fdw

external-table:
	make -C external-table

fdw:
ifneq ($(FDW_SUPPORT),)
	make -C fdw
endif

cli:
	make -C cli/go/src/pxf-cli

server:
	make -C server

clean:
	rm -rf build
	make -C $(SOURCE_EXTENSION_DIR) clean-all
	make -C cli/go/src/pxf-cli clean
	make -C server clean

test:
ifneq ($(FDW_SUPPORT),)
	make -C fdw test
endif
	make -C cli/go/src/pxf-cli test
	make -C server test

it:
	make -C automation TEST=$(TEST)

install:
	make -C $(SOURCE_EXTENSION_DIR) install
	make -C cli/go/src/pxf-cli install
	make -C server install

install-server:
	make -C server install-server

stage:
	rm -rf build/stage
	make -C $(SOURCE_EXTENSION_DIR) stage
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	GP_MAJOR_VERSION=$$(cat $(SOURCE_EXTENSION_DIR)/build/metadata/gp_major_version) ;\
	GP_BUILD_ARCH=$$(cat $(SOURCE_EXTENSION_DIR)/build/metadata/build_arch) ;\
	PXF_PACKAGE_NAME=pxf-gpdb$${GP_MAJOR_VERSION}-$${PXF_VERSION}-$${GP_BUILD_ARCH} ;\
	mkdir -p build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a $(SOURCE_EXTENSION_DIR)/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a cli/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a server/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	echo $$(git rev-parse --verify HEAD) > build/stage/$${PXF_PACKAGE_NAME}/pxf/commit.sha ;\
	cp package/install_binary build/stage/$${PXF_PACKAGE_NAME}/install_component

tar: stage
	rm -rf build/dist
	mkdir -p build/dist
	tar -czf build/dist/$(shell ls build/stage).tar.gz -C build/stage $(shell ls build/stage)

rpm:
	make -C $(SOURCE_EXTENSION_DIR) stage
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	GP_MAJOR_VERSION=$$(cat $(SOURCE_EXTENSION_DIR)/build/metadata/gp_major_version) ;\
	PXF_MAIN_VERSION=$${PXF_VERSION//-SNAPSHOT/} ;\
	if [[ $${PXF_VERSION} == *"-SNAPSHOT" ]]; then PXF_RELEASE=SNAPSHOT; else PXF_RELEASE=1; fi ;\
	rm -rf build/rpmbuild ;\
	mkdir -p build/rpmbuild/{BUILD,RPMS,SOURCES,SPECS} ;\
	mkdir -p build/rpmbuild/SOURCES/$(TARGET_EXTENSION_DIR) ;\
	cp -a $(SOURCE_EXTENSION_DIR)/build/stage/* build/rpmbuild/SOURCES/$(TARGET_EXTENSION_DIR) ;\
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
	GP_MAJOR_VERSION=$$(cat $(SOURCE_EXTENSION_DIR)/build/metadata/gp_major_version) ;\
	PXF_RPM_FILE=$$(find build/rpmbuild/RPMS -name pxf-gp$${GP_MAJOR_VERSION}-*.rpm) ;\
	PXF_RPM_BASE_NAME=$$(basename $${PXF_RPM_FILE%*.rpm}) ;\
	PXF_PACKAGE_NAME=$${PXF_RPM_BASE_NAME%.*} ;\
	mkdir -p build/stagerpm/$${PXF_PACKAGE_NAME} ;\
	cp $${PXF_RPM_FILE} build/stagerpm/$${PXF_PACKAGE_NAME} ;\
	cp package/install_rpm build/stagerpm/$${PXF_PACKAGE_NAME}/install_component ;\
	tar -czf build/distrpm/$${PXF_PACKAGE_NAME}.tar.gz -C build/stagerpm $${PXF_PACKAGE_NAME}

deb:
	make -C $(SOURCE_EXTENSION_DIR) stage
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	GP_MAJOR_VERSION=$$(cat $(SOURCE_EXTENSION_DIR)/build/metadata/gp_major_version) ;\
	PXF_MAIN_VERSION=$${PXF_VERSION//-SNAPSHOT/} ;\
	if [[ $${PXF_VERSION} == *"-SNAPSHOT" ]]; then PXF_RELEASE=SNAPSHOT; else PXF_RELEASE=1; fi ;\
	rm -rf build/debbuild ;\
	mkdir -p build/debbuild/usr/local/pxf-gp$${GP_MAJOR_VERSION}/$(TARGET_EXTENSION_DIR) ;\
	cp -a $(SOURCE_EXTENSION_DIR)/build/stage/* build/debbuild/usr/local/pxf-gp$${GP_MAJOR_VERSION}/$(TARGET_EXTENSION_DIR) ;\
	cp -a cli/build/stage/pxf/* build/debbuild/usr/local/pxf-gp$${GP_MAJOR_VERSION} ;\
	cp -a server/build/stage/pxf/* build/debbuild/usr/local/pxf-gp$${GP_MAJOR_VERSION} ;\
	echo $$(git rev-parse --verify HEAD) > build/debbuild/usr/local/pxf-gp$${GP_MAJOR_VERSION}/commit.sha ;\
	mkdir build/debbuild/DEBIAN ;\
	cp -a package/DEBIAN/* build/debbuild/DEBIAN/ ;\
	sed -i -e "s/%VERSION%/$${PXF_MAIN_VERSION}-$${PXF_RELEASE}/" -e "s/%MAINTAINER%/${VENDOR}/" build/debbuild/DEBIAN/control ;\
	dpkg-deb --build build/debbuild ;\
	mv build/debbuild.deb build/pxf-gp$${GP_MAJOR_VERSION}-$${PXF_MAIN_VERSION}-$${PXF_RELEASE}-ubuntu18.04-amd64.deb

deb-tar: deb
	rm -rf build/{stagedeb,distdeb}
	mkdir -p build/{stagedeb,distdeb}
	set -e ;\
	GP_MAJOR_VERSION=$$(cat $(SOURCE_EXTENSION_DIR)/build/metadata/gp_major_version) ;\
	PXF_DEB_FILE=$$(find build/ -name pxf-gp$${GP_MAJOR_VERSION}*.deb) ;\
	PXF_PACKAGE_NAME=$$(dpkg-deb --field $${PXF_DEB_FILE} Package)-$$(dpkg-deb --field $${PXF_DEB_FILE} Version)-ubuntu18.04 ;\
	mkdir -p build/stagedeb/$${PXF_PACKAGE_NAME} ;\
	cp $${PXF_DEB_FILE} build/stagedeb/$${PXF_PACKAGE_NAME} ;\
	cp package/install_deb build/stagedeb/$${PXF_PACKAGE_NAME}/install_component ;\
	tar -czf build/distdeb/$${PXF_PACKAGE_NAME}.tar.gz -C build/stagedeb $${PXF_PACKAGE_NAME}


help:
	@echo
	@echo 'Possible targets'
	@echo	'  - all (external-table, cli, server)'
	@echo	'  - external-table - build Greenplum external table extension'
	@echo	'  - fdw - build PXF Foreign-Data Wrapper Greenplum extension'
	@echo	'  - cli - install Go CLI dependencies and build Go CLI'
	@echo	'  - server - install PXF server dependencies and build PXF server'
	@echo	'  - clean - clean up external-table, fdw, CLI and server binaries'
	@echo	'  - test - runs tests for PXF Go CLI and server'
	@echo	'  - install - install PXF external table extension, CLI and server'
	@echo	'  - install-server - install PXF server without running tests'
	@echo	'  - tar - bundle PXF external table extension, CLI, server and tomcat into a single tarball'
	@echo	'  - rpm - create PXF RPM package'
	@echo	'  - rpm-tar - bundle PXF RPM package along with helper scripts into a single tarball'
	@echo	'  - deb - create PXF DEB package'
	@echo	'  - deb-tar - bundle PXF DEB package along with helper scripts into a single tarball'
