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

default: all

BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"

help:
	@echo
	@echo   "Possible targets"
	@echo	"  - all (clean, compile, test, install, tar, doc)"
	@echo	"  - clean - cleans the workspace of all produced artifacts"
	@echo	"  - compile - compiles all PXF modules and builds their JAR files"
	@echo	"  - test - runs unit tests for all PXF modules"
	@echo	"  - install - setup PXF along with tomcat in the configured deployPath"
	@echo	"  - tar - bundle PXF along with tomcat into a single tarball"
	@echo	"  - doc - creates aggregate javadoc under docs"

all:
	./gradlew clean tar aggregateJavadoc $(BUILD_PARAMS)

compile:
	./gradlew compileJava

test unittest:
	./gradlew test

tar:
	./gradlew tar ${BUILD_PARAMS}

clean:
	./gradlew clean
	rm -rf build

distclean maintainer-clean: clean

doc:
	./gradlew aggregateJavadoc

.PHONY: install
install:
	@if [[ "$(PXF_HOME)" == "" ]]; then \
		echo "ERROR: PXF_HOME is not set correctly"; exit 2; \
	fi
	./gradlew install ${BUILD_PARAMS}

.PHONY: version
version:
	@./gradlew -q version
