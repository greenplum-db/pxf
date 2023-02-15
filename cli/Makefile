include ../common.mk

.PHONY: all build stage install test clean clean-all help
.DEFAULT_GOAL := build

MODULE_NAME    := $(shell go list -m)
TARGET         := pxf-cli
PXF_ROOT_DIR   := $(shell cd .. && pwd)
PXF_SERVER_DIR := $(PXF_ROOT_DIR)/server
PXF_CLI_DIR    := $(PXF_ROOT_DIR)/cli
PXF_VERSION    ?= $(shell cat $(PXF_ROOT_DIR)/version)
GOMODCACHE     ?= $(shell go env GOMODCACHE)

export GOBIN   := $(CURDIR)/bin

LDFLAGS := -ldflags "-X $(MODULE_NAME)/cmd.version=$(PXF_VERSION)"

help:
	@echo
	@echo   "Possible targets"
	@echo	"  - all (build, test, stage, install)"
	@echo	"  - build - build pxf-cli binary"
	@echo	"  - test - runs tests for pxf-cli binary"
	@echo	"  - stage - install pxf-cli binary into build/stage/bin directory"
	@echo	"  - install - install pxf-cli binary into $PXF_HOME/bin/"
	@echo	"  - clean - remove pxf-cli binary"

all: test stage install

build:
	@if [ -d ${HOME}/.go-mod-cached-sources ] && [ ! -d $(GOMODCACHE) ]; then \
		mkdir -p $(GOMODCACHE); \
		ln -s ${HOME}/.go-mod-cached-sources $(GOMODCACHE); \
	fi
	go build -v $(LDFLAGS) -o build/$(TARGET) $(MODULE_NAME)

stage: test
	mkdir -p $(PXF_CLI_DIR)/build/stage/bin
	cp build/$(TARGET) build/stage/bin

install: build
	@if [ -z "$(PXF_HOME)" ]; then \
		echo "ERROR: PXF_HOME is not set"; exit 2; \
	fi
	mkdir -p $(PXF_HOME)/bin
	cp build/$(TARGET) $(PXF_HOME)/bin

test: build install-tools
	${GOBIN}/ginkgo cmd end_to_end

install-tools:
	@if [ ! -x ${GOBIN}/ginkgo ]; then \
		cat tools.go | awk -F'"' '/_/{print $$2}' | xargs -tI % go install %; \
	fi

clean:
	rm -rf $(PXF_CLI_DIR)/build
	go clean -i

clean-all: clean
