.PHONY: depend build test
.DEFAULT_GOAL := build

export GOPATH := $(shell cd ../.. && pwd)
TARGET := $(shell echo $${PWD\#\#*/})
GIT_VERSION := $(shell git describe --tags | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
BACKUP_VERSION_STR="-X $(TARGET)/cmd.version=$(GIT_VERSION)"
LDFLAGS=-ldflags $(BACKUP_VERSION_STR)

depend:
	@dep ensure -v

build: depend
	go build -v $(LDFLAGS)
	go install $(LDFLAGS)

test: build
	ginkgo pxf cmd end_to_end

clean :
	go clean -i
