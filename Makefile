PXF_VERSION := $(shell grep '^version=' ./server/gradle.properties | cut -d "=" -f2)

ifneq "$(PXF_HOME)" ""
	BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"
else ifneq "$(GPHOME)" ""
	PXF_HOME= "$(GPHOME)/pxf"
	BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"
endif

export PXF_HOME PXF_VERSION BUILD_PARAMS

default: all

.PHONY: all cli server install tar clean

all: cli server

cli:
	make -C cli/go/src/pxf-cli

server:
	make -C server

clean:
	make -C cli/go/src/pxf-cli clean
	make -C server clean

tar:
	make -C cli/go/src/pxf-cli tar
	make -C server tar

install:
	make -C cli/go/src/pxf-cli install
	make -C server install
