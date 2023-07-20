include ../../../../common.mk

.PHONY: all build stage install test clean help
.DEFAULT_GOAL := build

TARGET := $(shell echo $${PWD\#\#*/})
PXF_ROOT_DIR := $(shell cd ../../../.. && pwd)
PXF_SERVER_DIR := ${PXF_ROOT_DIR}/server
PXF_CLI_DIR := ${PXF_ROOT_DIR}/cli
PXF_VERSION ?= $(shell cat $(PXF_ROOT_DIR)/version)

LDFLAGS := -ldflags "-X ${TARGET}/cmd.version=${PXF_VERSION}"
GOPATH := ${PXF_ROOT_DIR}/cli/go

help:
	@echo
	@echo   "Possible targets"
	@echo	"  - all (build, test, stage, install)"
	@echo	"  - build - build pxf-cli binary"
	@echo	"  - test - runs tests for pxf-cli binary"
	@echo	"  - stage - install pxf-cli binary into build/stage/pxf/bin directory"
	@echo	"  - install - install pxf-cli binary into $PXF_HOME/bin/"
	@echo	"  - clean - remove pxf-cli binary"

all: test stage install

build:
	@if [ -d ${HOME}/.go-mod-cached-sources ] && [ ! -d $(GO_MOD_CACHE)/mod ]; then \
		mkdir -p $(GO_MOD_CACHE); \
		ln -s ${HOME}/.go-mod-cached-sources $(GO_MOD_CACHE)/mod; \
	fi
	go build -v ${LDFLAGS}
	go install ${LDFLAGS}

stage: test
	mkdir -p ${PXF_CLI_DIR}/build/stage/pxf/bin
	cp ${GOPATH}/bin/pxf-cli ${PXF_CLI_DIR}/build/stage/pxf/bin

install: build
	@if [ -z "$(PXF_HOME)" ]; then \
		echo "ERROR: PXF_HOME is not set"; exit 2; \
	fi
	mkdir -p $(PXF_HOME)/bin
	cp $(GOPATH)/bin/pxf-cli $(PXF_HOME)/bin

test: build
	# as the docker images no longer have ginkgo,
	# fetch and run ginkgo but don't actually install it
	go run github.com/onsi/ginkgo/ginkgo cmd end_to_end

clean:
	rm -rf ${PXF_CLI_DIR}/build
	go clean -i
