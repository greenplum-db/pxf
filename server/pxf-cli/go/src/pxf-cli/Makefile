.PHONY: all depend build tar install test clean
.DEFAULT_GOAL := build

export GOPATH := $(shell cd ../.. && pwd)
PXF_STAGING_DIR := $(shell cd ../../../../build/stage/bin && pwd)
PXF_SERVICE_SCRIPTS := $(shell cd ../../../../../server/pxf-service/src/scripts && pwd)
TARGET := $(shell echo $${PWD\#\#*/})
SERVER := $(shell cd ../../../../../server && pwd)
PXF_VERSION := $(shell grep version= ${SERVER}/gradle.properties | cut -d= -f2)
BACKUP_VERSION_STR := "-X ${TARGET}/cmd.version=${PXF_VERSION}"
LDFLAGS := -ldflags ${BACKUP_VERSION_STR}

all: test tar install

depend:
	@dep ensure -v

build: depend
	go build -v ${LDFLAGS}
	go install ${LDFLAGS}

tar: build
	mkdir -p ${PXF_STAGING_DIR}
	cp ${PXF_SERVICE_SCRIPTS}/pxf-service ${PXF_STAGING_DIR}/pxf
	cp ${GOPATH}/bin/pxf-cli ${PXF_STAGING_DIR}

install: build
	mkdir -p ${PXF_HOME}/bin
	cp ${PXF_SERVICE_SCRIPTS}/pxf-service ${PXF_HOME}/bin/pxf
	cp ${GOPATH}/bin/pxf-cli ${PXF_HOME}/bin

test: build
	ginkgo pxf cmd end_to_end

clean:
	go clean -i
