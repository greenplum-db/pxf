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

ifneq "$(HD)" ""
    BUILD_PARAMS+= -Dhd=$(HD)
else
    ifneq "$(PXF_HOME)" ""
        BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"
    else ifneq "$(GPHOME)" ""
        PXF_HOME= "$(GPHOME)/pxf"
        BUILD_PARAMS+= -DdeployPath="$(PXF_HOME)"
    else
		@echo "Cannot invoke install without configuring either PXF_HOME or GPHOME"
		exit
    endif
endif

ifneq "$(LICENSE)" ""
    BUILD_PARAMS+= -Plicense="$(LICENSE)"
endif

ifneq "$(VENDOR)" ""
    BUILD_PARAMS+= -Pvendor="$(VENDOR)"
endif

ifneq "$(PXF_VERSION)" ""
    BUILD_PARAMS+= -Pversion="$(PXF_VERSION)"
endif

ifneq "$(DATABASE)" ""
    BUILD_PARAMS+= -Ddatabase="$(DATABASE)"
endif

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
	./gradlew tar $(BUILD_PARAMS)

clean:
	./gradlew clean

distclean maintainer-clean: clean

doc:
	./gradlew aggregateJavadoc

.PHONY: install
install:
	./gradlew install $(BUILD_PARAMS)
	@if [ -d "$(PXF_HOME)/lib" ] ; then \
		cd $(PXF_HOME)/lib && \
		for X in pxf-*-[0-9]*.jar; do \
			ln -sf $$X `echo $$X | sed -e 's/-[a-zA-Z0-9.]*.jar/.jar/'`; \
		done; \
	fi
