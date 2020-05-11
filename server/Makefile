# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# protect the default target for this file from the targets in Makefile.global

include ../common.mk

default: stage-notest

PXF_ROOT_DIR := $(shell cd .. && pwd)
PXF_VERSION ?= $(shell cat $(PXF_ROOT_DIR)/version)

help:
	@echo
	@echo   "Possible targets"
	@echo	"  - all (clean, compile, test, stage)"
	@echo	"  - clean - cleans the workspace of all produced artifacts"
	@echo	"  - compile - compiles all PXF modules and builds their JAR files"
	@echo	"  - test - runs unit tests for all PXF modules"
	@echo	"  - install - setup PXF in the configured deployPath"
	@echo	"  - stage - build PXF server and stage resulting artifacts for packaging"
	@echo	"  - doc - creates aggregate javadoc under docs"

all:
	./gradlew -Pversion=$(PXF_VERSION) clean stage

compile:
	./gradlew -Pversion=$(PXF_VERSION) compileJava

GRADLEW_TEST_PARAMS = test
ifneq "$(TEST)" ""
	# find test file, remove leading "./" and trailing ".java"
	TEST_FILE = $(shell : "$$(find . -name $(TEST).java)"; : "$${_%.java}"; echo "$${_\#./}")
	# parse out the gradle project by grabbing top-level dir name
	PROJECT   = $(shell : '${TEST_FILE}'; echo "$${_%%/*}")
	# get java-style path with dots, starting at "org"
	TEST_PATH = $(shell : '${TEST_FILE}'; : "$${_\#${PROJECT}/src/test/java/}"; echo "$${_//\//.}")
	GRADLEW_TEST_PARAMS = :$(PROJECT):test --rerun-tasks --tests $(TEST_PATH)
endif

test unittest:
	@if [ -n '$(TEST)' ] && [ -z '${TEST_FILE}' ]; then \
		echo 'Test $(TEST) was not found'; \
		exit 1; \
	fi
	./gradlew -Pversion=$(PXF_VERSION) ${GRADLEW_TEST_PARAMS}

.PHONY: stage
stage: test
	./gradlew -Pversion=$(PXF_VERSION) stage

.PHONY: stage-notest
stage-notest:
	./gradlew -Pversion=$(PXF_VERSION) stage -x test

clean:
	./gradlew clean
	rm -rf build

distclean maintainer-clean: clean

doc:
	./gradlew -Pversion=$(PXF_VERSION) aggregateJavadoc

.PHONY: install
install: stage
	@if [ -z "$(PXF_HOME)" ]; then \
		echo "ERROR: PXF_HOME is not set"; exit 2; \
	fi
	cp -R build/stage/pxf/* "$(PXF_HOME)"

.PHONY: version
version:
	@./gradlew -q version
