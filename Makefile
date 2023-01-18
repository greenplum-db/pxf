include common.mk

PXF_VERSION ?= $(shell cat version)
export PXF_VERSION

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

ifndef PGXS
    $(error Make sure the Greenplum installation binaries are in your PATH. i.e. export PATH=<path to your greenplum installation>/bin:$$PATH)
endif

include $(PGXS)

FDW_BUILD = TRUE
FDW_PACKAGE = TRUE

ifeq ($(shell test $(GP_MAJORVERSION) -lt 6; echo $$?),0)
	FDW_BUILD = FALSE
endif

ifeq ($(shell test $(GP_MAJORVERSION) -lt 7; echo $$?),0)
	FDW_PACKAGE = FALSE
endif

GP_BUILD_ARCH = $(subst _,-,$(BLD_ARCH))

export GP_MAJORVERSION
export GP_BUILD_ARCH
export FDW_BUILD
export FDW_PACKAGE

LICENSE ?= ASL 2.0
VENDOR ?= Open Source

default: all

.PHONY: all extensions external-table fdw cli server install install-server stage tar rpm rpm-tar deb deb-tar clean test it help

all: extensions cli server

extensions: external-table fdw

external-table:
	make -C external-table

fdw:
ifeq ($(FDW_BUILD),TRUE)
	make -C fdw
else
	@echo "Skipping building FDW extension because GPDB version is less than 6"
endif

cli:
	make -C cli/go/src/pxf-cli

server:
	make -C server

clean:
	rm -rf build
	make -C external-table clean-all
	make -C fdw clean-all
	make -C cli/go/src/pxf-cli clean
	make -C server clean

test:
ifeq ($(FDW_BUILD),TRUE)
	make -C fdw installcheck
else
	@echo "Skipping testing FDW extension because GPDB version is less than 6"
endif
	make -C cli/go/src/pxf-cli test
	make -C server test

it:
	make -C automation TEST=$(TEST)

install:
	make -C external-table install
ifeq ($(FDW_BUILD),TRUE)
	make -C fdw install
else
	@echo "Skipping installing FDW extension because GPDB version is less than 6"
endif
	make -C cli/go/src/pxf-cli install
	make -C server install

install-server:
	make -C server install-server

stage:
	rm -rf build/stage
	make -C external-table stage
ifeq ($(FDW_PACKAGE),TRUE)
	make -C fdw stage
else
	@echo "Skipping staging FDW extension because GPDB version is less than 7"
endif
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	@echo GP_MAJORVERSION=$(GP_MAJORVERSION)
	@echo BLD_ARCH=$(BLD_ARCH)
	@echo GP_BUILD_ARCH=$(GP_BUILD_ARCH)
	set -e ;\
	PXF_PACKAGE_NAME=pxf-gpdb$${GP_MAJORVERSION}-$${PXF_VERSION}-$${GP_BUILD_ARCH} ;\
	mkdir -p build/stage/$${PXF_PACKAGE_NAME} ;\
	cp -a external-table/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	if [[ $${FDW_PACKAGE} == TRUE ]]; then \
	cp -a fdw/build/stage/* build/stage/$${PXF_PACKAGE_NAME} ;\
	fi;\
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
ifeq ($(FDW_PACKAGE),TRUE)
	make -C fdw stage
else
	@echo "Skipping packaging FDW extension because GPDB version is less than 7"
endif
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	PXF_MAIN_VERSION=$${PXF_VERSION//-SNAPSHOT/} ;\
	if [[ $${PXF_VERSION} == *"-SNAPSHOT" ]]; then PXF_RELEASE=SNAPSHOT; else PXF_RELEASE=1; fi ;\
	rm -rf build/rpmbuild ;\
	mkdir -p build/rpmbuild/{BUILD,RPMS,SOURCES,SPECS} ;\
	mkdir -p build/rpmbuild/SOURCES/gpextable ;\
	cp -a external-table/build/stage/* build/rpmbuild/SOURCES/gpextable ;\
	if [[ $${FDW_PACKAGE} == TRUE ]]; then \
	mkdir -p build/rpmbuild/SOURCES/fdw ;\
	cp -a fdw/build/stage/* build/rpmbuild/SOURCES/fdw ;\
	fi;\
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
	-bb $${PWD}/build/rpmbuild/SPECS/pxf-gp$${GP_MAJORVERSION}.spec

rpm-tar: rpm
	rm -rf build/{stagerpm,distrpm}
	mkdir -p build/{stagerpm,distrpm}
	set -e ;\
	PXF_RPM_FILE=$$(find build/rpmbuild/RPMS -name pxf-gp$${GP_MAJORVERSION}-*.rpm) ;\
	PXF_RPM_BASE_NAME=$$(basename $${PXF_RPM_FILE%*.rpm}) ;\
	PXF_PACKAGE_NAME=$${PXF_RPM_BASE_NAME%.*} ;\
	mkdir -p build/stagerpm/$${PXF_PACKAGE_NAME} ;\
	cp $${PXF_RPM_FILE} build/stagerpm/$${PXF_PACKAGE_NAME} ;\
	cp package/install_rpm build/stagerpm/$${PXF_PACKAGE_NAME}/install_component ;\
	tar -czf build/distrpm/$${PXF_PACKAGE_NAME}.tar.gz -C build/stagerpm $${PXF_PACKAGE_NAME}

deb:
	make -C external-table stage
ifeq ($(FDW_PACKAGE),TRUE)
	make -C fdw stage
else
	@echo "Skipping packaging FDW extension because GPDB version is less than 7"
endif
	make -C cli/go/src/pxf-cli stage
	make -C server stage
	set -e ;\
	PXF_MAIN_VERSION=$${PXF_VERSION//-SNAPSHOT/} ;\
	if [[ $${PXF_VERSION} == *"-SNAPSHOT" ]]; then PXF_RELEASE=SNAPSHOT; else PXF_RELEASE=1; fi ;\
	rm -rf build/debbuild ;\
	mkdir -p build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION}/gpextable ;\
	cp -a external-table/build/stage/* build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION}/gpextable ;\
	if [[ $${FDW_PACKAGE} == TRUE ]]; then \
	mkdir -p build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION}/fdw ;\
	cp -a fdw/build/stage/* build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION}/fdw ;\
	fi;\
	cp -a cli/build/stage/pxf/* build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION} ;\
	cp -a server/build/stage/pxf/* build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION} ;\
	echo $$(git rev-parse --verify HEAD) > build/debbuild/usr/local/pxf-gp$${GP_MAJORVERSION}/commit.sha ;\
	mkdir build/debbuild/DEBIAN ;\
	cp -a package/DEBIAN/* build/debbuild/DEBIAN/ ;\
	sed -i -e "s/%VERSION%/$${PXF_MAIN_VERSION}-$${PXF_RELEASE}/" -e "s/%MAINTAINER%/${VENDOR}/" build/debbuild/DEBIAN/control ;\
	dpkg-deb --build build/debbuild ;\
	mv build/debbuild.deb build/pxf-gp$${GP_MAJORVERSION}-$${PXF_MAIN_VERSION}-$${PXF_RELEASE}-ubuntu18.04-amd64.deb

deb-tar: deb
	rm -rf build/{stagedeb,distdeb}
	mkdir -p build/{stagedeb,distdeb}
	set -e ;\
	PXF_DEB_FILE=$$(find build/ -name pxf-gp$${GP_MAJORVERSION}*.deb) ;\
	PXF_PACKAGE_NAME=$$(dpkg-deb --field $${PXF_DEB_FILE} Package)-$$(dpkg-deb --field $${PXF_DEB_FILE} Version)-ubuntu18.04 ;\
	mkdir -p build/stagedeb/$${PXF_PACKAGE_NAME} ;\
	cp $${PXF_DEB_FILE} build/stagedeb/$${PXF_PACKAGE_NAME} ;\
	cp package/install_deb build/stagedeb/$${PXF_PACKAGE_NAME}/install_component ;\
	tar -czf build/distdeb/$${PXF_PACKAGE_NAME}.tar.gz -C build/stagedeb $${PXF_PACKAGE_NAME}


help:
	@echo
	@echo 'Possible targets'
	@echo	'  - all (extensions, cli, server)'
	@echo	'  - extensions - build external table, fdw extensions'
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
