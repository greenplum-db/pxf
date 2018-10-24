.PHONY: depend build test
.DEFAULT_GOAL := build

depend:
	@GOPATH=$(shell pwd)/../.. dep ensure -v

build: depend
	@GOPATH=$(shell pwd)/../.. go build

test:
	@GOPATH=$(shell pwd)/../.. ginkgo test