.PHONY: depend build test
.DEFAULT_GOAL := build

GOPATH=$(shell pwd)/../..
GIT_VERSION := $(shell git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
BACKUP_VERSION_STR="-X pxf-cli/cmd.version=$(GIT_VERSION)"

depend:
	@GOPATH=$(GOPATH) dep ensure -v

build: depend
	@GOPATH=$(GOPATH) go build -ldflags $(BACKUP_VERSION_STR)

test:
	@GOPATH=$(GOPATH) ginkgo pxf cmd

clean :
	rm -f pxf-cli
