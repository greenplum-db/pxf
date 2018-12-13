# In case you wonder what to do with this
# make ===> run all tests
# make GROUP=<groupname> ===> run test group <groupname>
# make TEST=<testname> ===> run <testname>.class
# make TEST=<class>#<method> ===> run specific <method> from <class>.class
# make OFFLINE=true ===> run using only cache

MAVEN_TEST_OPTS+= -B -e

ifneq "$(TEST)" ""
	MAVEN_TEST_OPTS+= -Dtest=$(TEST)
endif

ifneq "$(GROUP)" ""
	MAVEN_TEST_OPTS+= -Dgroups=$(GROUP)
endif

ifeq "$(PXF_HOME)" ""
	PXF_HOME=$(GPHOME)/pxf
endif
MAVEN_TEST_OPTS+= -DPXF_TMP_LIB=$(PXF_HOME)/tmp

ifneq "$(OFFLINE)" "true"
	MAVEN_TEST_OPTS+= -U
else
	MAVEN_TEST_OPTS+= -o
endif

ifneq "$(PROTOCOL)" ""
	MAVEN_TEST_OPTS+= -DPROTOCOL=$(PROTOCOL)
	PROTOCOL_HOME=~/pxf/servers/$(PROTOCOL)
endif

#MAVEN_DEBUG_OPTS+= -Dmaven.surefire.debug

MVN=mvn

.PHONY: all test

all: test

symlink_pxf_jars:
	@if [ -d "$(PXF_HOME)/lib" ]; then \
		rm -rf $(PXF_HOME)/tmp && \
		mkdir -p $(PXF_HOME)/tmp && \
		cd $(PXF_HOME)/tmp && \
		for X in $(PXF_HOME)/lib/pxf-*-[0-9]*.jar; do \
			ln -sf $$X `echo \`basename $$X\` | sed -e 's/-[a-zA-Z0-9.]*.jar/.jar/'`; \
		done; \
	fi

test: symlink_pxf_jars sync_cloud_configs
	$(MVN) $(MAVEN_TEST_OPTS) ${MAVEN_DEBUG_OPTS} test

clean:
	$(MVN) $(MAVEN_TEST_OPTS) clean
	rm -rf $(PXF_HOME)/tmp

clean-logs:
	rm -rf automation_logs/*
	rm -rf tincrepo/main/log/*
	rm -rf run-results/*
	find tincrepo/main/pxf/. -type d -name "output" -exec rm -rf {} +
	find tincrepo/main/pxf/. -type f -name "*segment_logs" -exec rm -rf {} +

dev: symlink_pxf_jars
	$(MVN) $(MAVEN_TEST_OPTS) package -DskipTests=true

sync_cloud_configs:
ifneq "$(PROTOCOL)" ""
	@mkdir -p $(PROTOCOL_HOME)
	@if [[ ! -f "$(PROTOCOL_HOME)/$(PROTOCOL)-site.xml" ]]; then \
		cp $(PXF_HOME)/templates/user/templates/$(PROTOCOL)-site.xml $(PROTOCOL_HOME)/; \
		cp $(PXF_HOME)/templates/user/templates/mapred-site.xml $(PROTOCOL_HOME)/; \
		if [[ $(PROTOCOL) == s3 ]]; then \
			if [[ -z "$(AWS_ACCESS_KEY_ID)" ]] || [[ -z "$(AWS_SECRET_ACCESS_KEY)" ]]; then \
				echo "AWS Keys (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) not set"; \
				rm -rf $(PROTOCOL_HOME); \
				exit 1; \
			fi; \
			sed -i "s|</configuration>|<property><name>fs.defaultFS</name><value>s3a://gpdb-ud-scratch</value></property></configuration>|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			sed -i "s|YOUR_AWS_ACCESS_KEY_ID|$(AWS_ACCESS_KEY_ID)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			sed -i "s|YOUR_AWS_SECRET_ACCESS_KEY|$(AWS_SECRET_ACCESS_KEY)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
		fi; \
		echo "Created $(PROTOCOL) server configuration"; \
	fi
endif

list-tests:
ifneq ("$(TEST)", "")
	@echo "***** Tests in $(TEST): *****"
	@find src/ -name "$(TEST).java" -exec grep "@Test" -A 1 {} \; | grep "public" | awk '{print "* ", $$3}'
else
	@echo "***** Smoke tests: *****"
	@ls src/test/java/org/greenplum/pxf/automation/smoke/*Test.java | sed 's/.*\///g' | sed 's/\.java//g' | awk '{print "* ", $$1}'
	@echo "***** Feature tests: *****"
	@ls src/test/java/org/greenplum/pxf/automation/features/*/*Test.java | sed 's/.*\///g' | sed 's/\.java//g' | awk '{print "* ", $$1}'
endif
