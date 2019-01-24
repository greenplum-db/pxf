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
TOMCAT_VERSION="7.0.62"
TOMCAT_DIR="tomcat/build"
TOMCAT_TAR="apache-tomcat-${TOMCAT_VERSION}.tar.gz"
TOMCAT_URL="http://archive.apache.org/dist/tomcat/tomcat-7/v${TOMCAT_VERSION}/bin/${TOMCAT_TAR}"

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
	./gradlew clean tar $(BUILD_PARAMS)

compile:
	./gradlew compileJava

test unittest:
	./gradlew test

tar: tomcat
	./gradlew tar ${BUILD_PARAMS}

clean:
	./gradlew clean
	rm -rf build

distclean maintainer-clean: clean

doc:
	./gradlew aggregateJavadoc

.PHONY: tomcat
tomcat:
	@if [[ -f "${TOMCAT_DIR}/${TOMCAT_TAR}" ]]; then \
		echo "${TOMCAT_TAR} already exists, nothing to do"; \
	else \
		echo "Downloading Tomcat from ${TOMCAT_URL}..."; \
		mkdir -p ${TOMCAT_DIR}; \
		wget -q ${TOMCAT_URL} -P ${TOMCAT_DIR}; \
	fi
	@if [[ ! -d "${TOMCAT_DIR}/apache-tomcat" ]]; then \
		mkdir -p ${TOMCAT_DIR}/apache-tomcat; \
		echo "Extracting Tomcat into ${TOMCAT_DIR}..."; \
		pushd ${TOMCAT_DIR} > /dev/null; \
		tar -xzf ${TOMCAT_TAR} -C apache-tomcat --strip-components 1; \
		popd > /dev/null; \
	fi

.PHONY: install
install: tomcat
	@if [[ "$(PXF_HOME)" == "" ]]; then \
		echo "ERROR: PXF_HOME is not set correctly"; exit 2; \
	fi
	./gradlew install ${BUILD_PARAMS}

.PHONY: version
version:
	@./gradlew -q version
