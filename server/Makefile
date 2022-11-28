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
PXF_API_VERSION ?= $(shell cat $(PXF_ROOT_DIR)/api_version)

PXF_GRADLE_PROPERTIES = -Pversion=$(PXF_VERSION) -PapiVersion=$(PXF_API_VERSION)

help:
	@echo
	@echo   "Possible targets"
	@echo	"  - all (clean, compile, test, stage)"
	@echo	"  - clean - cleans the workspace of all produced artifacts"
	@echo	"  - compile - compiles all PXF modules and builds their JAR files"
	@echo	"  - test - runs unit tests for all PXF modules"
	@echo   "  - coverage - runs unit tests for all PXF modules and gives a coverage report"
	@echo	"  - install - setup PXF in the configured deployPath"
	@echo	"  - stage - build PXF server and stage resulting artifacts for packaging"
	@echo	"  - doc - creates aggregate javadoc under docs"

all:
	./gradlew $(PXF_GRADLE_PROPERTIES) clean stage

compile:
	./gradlew $(PXF_GRADLE_PROPERTIES) compileJava

GRADLEW_TEST_PARAMS = test
ifneq "$(TEST)" ""
	# find test file, remove leading "./" and trailing ".java"
	TEST_FILE = $(subst ./,,$(subst .java,,$(shell find . -name $(TEST).java)))
	# parse out the gradle project by grabbing top-level dir name
	PROJECT = $(word 1,$(subst /, ,$(TEST_FILE)))
	# get java-style path with dots, starting at "org"
	TEST_PATH = $(subst /,.,$(subst $(PROJECT)/src/test/java/,,$(TEST_FILE)))
	GRADLEW_TEST_PARAMS = :$(PROJECT):test --rerun-tasks --tests $(TEST_PATH)
endif

test unittest:
	@if [ -n '$(TEST)' ] && [ -z '${TEST_FILE}' ]; then \
		echo 'Test $(TEST) was not found'; \
		exit 1; \
	fi
	./gradlew $(PXF_GRADLE_PROPERTIES) ${GRADLEW_TEST_PARAMS}

coverage:
	./gradlew $(PXF_GRADLE_PROPERTIES) ${GRADLEW_TEST_PARAMS} jacocoTestReport
	@echo
	@echo  "Coverage reports can be found within each server module under <module-name>/build/reports/jacoco"
	@echo  "For example, the pxf-service coverage report is located at $(shell pwd)/pxf-service/build/reports/jacoco/test/html/index.html"

.PHONY: stage
stage:
	./gradlew $(PXF_GRADLE_PROPERTIES) test stage
	install -m 744 -d "build/stage/pxf/lib"
	install -m 744 -d "build/stage/pxf/lib/native"
	install -m 744 -d "build/stage/pxf/servers"
	install -m 744 -d "build/stage/pxf/servers/default"
	install -m 700 -d "build/stage/pxf/logs"
	install -m 700 -d "build/stage/pxf/run"
	install -m 700 -d "build/stage/pxf/keytabs"

.PHONY: stage-notest
stage-notest:
	./gradlew $(PXF_GRADLE_PROPERTIES) stage -x test
	install -m 744 -d "build/stage/pxf/lib"
	install -m 744 -d "build/stage/pxf/lib/native"
	install -m 744 -d "build/stage/pxf/servers"
	install -m 744 -d "build/stage/pxf/servers/default"
	install -m 700 -d "build/stage/pxf/logs"
	install -m 700 -d "build/stage/pxf/run"
	install -m 700 -d "build/stage/pxf/keytabs"

clean:
	./gradlew clean
	rm -rf build

distclean maintainer-clean: clean

doc:
	./gradlew $(PXF_GRADLE_PROPERTIES) aggregateJavadoc

.PHONY: install
install: stage
	@if [ -z "$(PXF_HOME)" ]; then \
		echo "ERROR: PXF_HOME is not set"; exit 2; \
	fi
	mkdir -p "$(PXF_HOME)"
	cp -R build/stage/pxf/* "$(PXF_HOME)"

.PHONY: install-server
install-server: stage-notest
	@if [ -z "$(PXF_HOME)" ]; then \
		echo "ERROR: PXF_HOME is not set"; exit 2; \
	fi
	mkdir -p "$(PXF_HOME)"
	cp -R build/stage/pxf/* "$(PXF_HOME)"

.PHONY: version
version:
	@./gradlew -q version
