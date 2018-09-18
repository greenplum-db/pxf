# In case you wonder what to do with this
# make ===> run all tests
# make GROUP=<groupname> ===> run test group <groupname>
# make TEST=<testname> ===> run <testname>.class
# make TEST=<class>#<method> ===> run specific <method> from <class>.class
# make OFFLINE=true ===> run using only cache

MAVEN_TEST_OPTS+= -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -B -e

ifneq "$(TEST)" ""
    MAVEN_TEST_OPTS+= -Dtest=$(TEST)
endif

ifneq "$(GROUP)" ""
    MAVEN_TEST_OPTS+= -Dgroups=$(GROUP)
endif

ifeq "$(PG_MODE)" "HAWQ"
    MAVEN_TEST_OPTS+= -DPXF_LIB=$(GPHD_ROOT)/pxf
else
	# Default PG_MODE is GPDB
	ifeq "$(PXF_HOME)" ""
		PXF_HOME=$(GPHOME)/pxf
	endif
    MAVEN_TEST_OPTS+= -DPG_MODE=$(PG_MODE)
    MAVEN_TEST_OPTS+= -DPXF_LIB=$(PXF_HOME)/lib
endif

ifneq "$(OFFLINE)" "true"
    MAVEN_TEST_OPTS+= -U
else
    MAVEN_TEST_OPTS+= -o
endif

#MAVEN_DEBUG_OPTS+= -Dmaven.surefire.debug

MVN=mvn

.PHONY: all test

all: test

test:
	$(MVN) $(MAVEN_TEST_OPTS) ${MAVEN_DEBUG_OPTS} test

clean:
	$(MVN) $(MAVEN_TEST_OPTS) clean

clean-logs:
	rm -rf automation_logs/*
	rm -rf tincrepo/main/log/*
	rm -rf run-results/*
	find tincrepo/main/pxf/. -type d -name "output" -exec rm -rf {} +
	find tincrepo/main/pxf/. -type f -name "*segment_logs" -exec rm -rf {} +

dev:
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
