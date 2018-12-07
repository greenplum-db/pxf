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

# To use a different protocol say s3
#MAVEN_TEST_OPTS+= -DPROTOCOL=s3

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

test: symlink_pxf_jars
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
