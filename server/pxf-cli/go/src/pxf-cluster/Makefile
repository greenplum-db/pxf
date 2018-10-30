.PHONY: depend build test
.DEFAULT_GOAL := build

GOPATH=$(shell pwd)/../..

depend:
	@GOPATH=$(GOPATH) dep ensure -v

build: depend
	@GOPATH=$(GOPATH) go build

test:
	@GOPATH=$(GOPATH) ginkgo test
