# In case you wonder what to do with this
# make ===> run all tests
# make GROUP=<groupname> ===> run test group <groupname>
# make TEST=<testname> ===> run <testname>.class
# make TEST=<class>#<method> ===> run specific <method> from <class>.class
# make OFFLINE=true ===> run using only cache

SHELL := bash
UNAME_S := $(shell uname -s)
MAVEN_TEST_OPTS+= -B -e
PXF_TMP_LIB := $(HOME)/automation_tmp_lib
BASE_PATH   ?= /mnt/nfs/var/nfsshare
USE_FDW     ?= false

ifneq "$(TEST)" ""
	MAVEN_TEST_OPTS+= -Dtest=$(TEST)
endif

ifneq "$(GROUP)" ""
	MAVEN_TEST_OPTS+= -Dgroups=$(GROUP)
endif

MAVEN_TEST_OPTS+= -Djava.awt.headless=true -DuseFDW=$(USE_FDW)

ifneq "$(OFFLINE)" "true"
	MAVEN_TEST_OPTS+= -U
else
	MAVEN_TEST_OPTS+= -o
endif

# lowercase the protocol
PROTOCOL := $(shell echo $(PROTOCOL) | tr A-Z a-z)

ifeq "$(PROTOCOL)" "minio"
	MINIO=true
	PROTOCOL=s3
endif

PXF_BASE ?= $(PXF_HOME)
TEMPLATES_DIR=$(PXF_HOME)/templates

PXF_BASE_SERVERS=$(PXF_BASE)/servers

ifneq "$(PROTOCOL)" ""
	MAVEN_TEST_OPTS+= -DPROTOCOL=$(PROTOCOL)
	PROTOCOL_HOME=$(PXF_BASE_SERVERS)/$(PROTOCOL)
endif

ifneq "$(ACCESS_KEY_ID)" ""
	MAVEN_TEST_OPTS+= -DAWS_ACCESS_KEY_ID=$(ACCESS_KEY_ID)
endif

ifneq "$(SECRET_ACCESS_KEY)" ""
	MAVEN_TEST_OPTS+= -DAWS_SECRET_ACCESS_KEY=$(SECRET_ACCESS_KEY)
endif

JDBC_HOST ?= localhost

ifeq "$(JDBC_PORT)" ""
	ifeq "$(PGPORT)" ""
		JDBC_PORT=15432
	else
		JDBC_PORT=$(PGPORT)
	endif
endif

HIVE_SERVER_HOST ?= localhost

HIVE_SERVER_PORT ?= 10000

SED_OPTS=-i
ifeq ($(UNAME_S),Darwin)
    SED_OPTS+= ""
endif

ifeq "$(PXF_TEST_KEEP_DATA)" "true"
	MAVEN_DEBUG_OPTS+= -DPXF_TEST_KEEP_DATA=$(PXF_TEST_KEEP_DATA)
endif

# set PXF_TEST_DEBUG to true to remotely debug pxf automation test
ifeq "$(PXF_TEST_DEBUG)" "true"
	MAVEN_DEBUG_OPTS+= -Dmaven.surefire.debug
endif

# disables credentials check by artifactregistry-maven-wagon plugin when run inside GCE VM.
export NO_GCE_CHECK := true

MVN=mvn

.PHONY: all test

all: test

check-env:
	@if [ -z "$(PXF_HOME)" ]; then echo 'ERROR: PXF_HOME must be set'; exit 1; fi

symlink_pxf_jars: check-env
	@if [ -d "$(PXF_HOME)/application" ]; then \
		rm -rf $(PXF_TMP_LIB) && \
		mkdir -p $(PXF_TMP_LIB)/tmp && \
		unzip -qq -j $(PXF_HOME)/application/pxf-app-*.jar 'BOOT-INF/lib/pxf-*.jar' -d $(PXF_TMP_LIB)/tmp && \
		for jar in $(PXF_TMP_LIB)/tmp/pxf-*.jar; do \
			jar_name="$${jar##*/}"; \
			if [[ $${jar_name} =~ ^pxf-[A-Za-z0-9]+(-[0-9.]+.*).jar$$ ]]; then \
				link=$(PXF_TMP_LIB)/$${jar_name/$${BASH_REMATCH[1]}/}; \
				echo "copying $${link} -> $${jar}"; \
				cp "$${jar}" "$${link}"; \
			fi; \
		done; \
		empty_dir=$$(mktemp -d) && \
		jar cf $(PXF_TMP_LIB)/pxf-extras.jar -C $${empty_dir} . && \
		rmdir $${empty_dir} && \
		if [ -f "/tmp/pxf-extras-1.0.0.jar" ]; then cp /tmp/pxf-extras-1.0.0.jar $(PXF_TMP_LIB)/pxf-extras.jar; fi && \
		rm -rf $(PXF_TMP_LIB)/tmp; \
	fi

test: check-env clean-logs symlink_pxf_jars sync_cloud_configs sync_jdbc_config
	$(MVN) $(MAVEN_TEST_OPTS) ${MAVEN_DEBUG_OPTS} test

clean: clean-logs
	$(MVN) $(MAVEN_TEST_OPTS) clean
	@rm -rf $(PXF_TMP_LIB)

clean-logs:
	@rm -rf automation_logs/* tincrepo/main/log/* run-results/*
	@find tincrepo/main/pxf/. -type d -name "output" -exec rm -rf {} +
	@find tincrepo/main/pxf/. -type f -name "*segment_logs" -exec rm -rf {} +

dev: symlink_pxf_jars
	$(MVN) $(MAVEN_TEST_OPTS) package -DskipTests=true

sync_jdbc_config:
	@mkdir -p $(PXF_BASE_SERVERS)/database
	@if [ ! -f $(PXF_BASE_SERVERS)/database/jdbc-site.xml ]; then \
		cp $(TEMPLATES_DIR)/jdbc-site.xml $(PXF_BASE_SERVERS)/database/; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|" $(PXF_BASE_SERVERS)/database/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://$(JDBC_HOST):$(JDBC_PORT)/pxfautomation|" $(PXF_BASE_SERVERS)/database/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_USER||" $(PXF_BASE_SERVERS)/database/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_PASSWORD||" $(PXF_BASE_SERVERS)/database/jdbc-site.xml; \
		cp $(PXF_BASE_SERVERS)/database/jdbc-site.xml $(PXF_BASE_SERVERS)/database/testuser-user.xml; \
		sed $(SED_OPTS) "s|pxfautomation|template1|" $(PXF_BASE_SERVERS)/database/testuser-user.xml; \
	fi
	@cp src/test/resources/report.sql $(PXF_BASE_SERVERS)/database
	@mkdir -p $(PXF_BASE_SERVERS)/db-session-params
	@if [ ! -f $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml ]; then \
		cp $(TEMPLATES_DIR)/jdbc-site.xml $(PXF_BASE_SERVERS)/db-session-params/; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|" $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://$(JDBC_HOST):$(JDBC_PORT)/pxfautomation|" $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_USER||" $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_PASSWORD||" $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml; \
		sed $(SED_OPTS) "s|</configuration>|<property><name>jdbc.session.property.client_min_messages</name><value>debug1</value></property></configuration>|"  $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml; \
		sed $(SED_OPTS) "s|</configuration>|<property><name>jdbc.session.property.default_statistics_target</name><value>123</value></property></configuration>|"  $(PXF_BASE_SERVERS)/db-session-params/jdbc-site.xml; \
	fi
	@mkdir -p $(PXF_BASE_SERVERS)/db-hive
	@if [ ! -f $(PXF_BASE_SERVERS)/db-hive/jdbc-site.xml ]; then \
		cp $(TEMPLATES_DIR)/jdbc-site.xml $(PXF_BASE_SERVERS)/db-hive/; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.apache.hive.jdbc.HiveDriver|" $(PXF_BASE_SERVERS)/db-hive/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_URL|jdbc:hive2://$(HIVE_SERVER_HOST):$(HIVE_SERVER_PORT)/default|" $(PXF_BASE_SERVERS)/db-hive/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_USER||" $(PXF_BASE_SERVERS)/db-hive/jdbc-site.xml; \
		sed $(SED_OPTS) "s|YOUR_DATABASE_JDBC_PASSWORD||" $(PXF_BASE_SERVERS)/db-hive/jdbc-site.xml; \
	fi
	@if [ ! -f $(PXF_BASE_SERVERS)/db-hive/hive-report.sql ]; then \
		cp src/test/resources/hive-report.sql $(PXF_BASE_SERVERS)/db-hive; \
	fi

sync_cloud_configs:
ifneq "$(PROTOCOL)" ""
	@mkdir -p $(PROTOCOL_HOME)
	@ set -e; \
	if [ ! -f "$(PROTOCOL_HOME)/$(PROTOCOL)-site.xml" ]; then \
		if [ $(PROTOCOL) != file ]; then \
			cp $(TEMPLATES_DIR)/$(PROTOCOL)-site.xml $(PROTOCOL_HOME)/; \
		fi; \
		cp $(TEMPLATES_DIR)/mapred-site.xml $(PROTOCOL_HOME)/; \
		if [ $(PROTOCOL) = file ]; then \
			if [ ! -d "$(BASE_PATH)" ]; then \
				echo "The NFS directory '$(BASE_PATH)' does not exist. Please configure it and try again"; \
				rm -rf $(PROTOCOL_HOME); \
				exit 1; \
			fi; \
			echo "Make sure your $$PXF_BASE/conf/pxf-profiles.xml file configures the file:AvroSequenceFile and file:SequenceFile profiles"; \
			cp $(TEMPLATES_DIR)/pxf-site.xml $(PROTOCOL_HOME)/; \
			sed $(SED_OPTS) 's|</configuration>|<property><name>pxf.fs.basePath</name><value>$(BASE_PATH)</value></property></configuration>|g' $(PROTOCOL_HOME)/pxf-site.xml; \
		fi; \
		if [ $(PROTOCOL) = s3 ]; then \
			if [ "$(MINIO)" = "true" ]; then \
				cp $(TEMPLATES_DIR)/minio-site.xml $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
				sed $(SED_OPTS) "s|YOUR_MINIO_URL|http://localhost:9000|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			fi; \
			mkdir -p $(PROTOCOL_HOME)-invalid; \
			cp $(TEMPLATES_DIR)/$(PROTOCOL)-site.xml $(PROTOCOL_HOME)-invalid/; \
			if [ -z "$(ACCESS_KEY_ID)" ] || [ -z "$(SECRET_ACCESS_KEY)" ]; then \
				echo "AWS Keys (ACCESS_KEY_ID, SECRET_ACCESS_KEY) not set"; \
				rm -rf $(PROTOCOL_HOME); \
				exit 1; \
			fi; \
			sed $(SED_OPTS) "s|YOUR_AWS_ACCESS_KEY_ID|$(ACCESS_KEY_ID)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			sed $(SED_OPTS) "s|YOUR_AWS_SECRET_ACCESS_KEY|$(SECRET_ACCESS_KEY)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
		fi; \
		if [ $(PROTOCOL) = adl ]; then \
			if [ -z "$(ADL_ACCOUNT)" ] || [ -z "$(ADL_REFRESH_URL)" ] || [ -z "$(ADL_CLIENT_ID)" ] || [ -z "$(ADL_CREDENTIAL)" ]; then \
				echo "ADL Keys (ADL_ACCOUNT, ADL_CLIENT_ID, ADL_CREDENTIAL, ADL_REFRESH_URL) not set"; \
				rm -rf $(PROTOCOL_HOME); \
				exit 1; \
			fi; \
			sed $(SED_OPTS) "s|YOUR_ADL_REFRESH_URL|$(ADL_REFRESH_URL)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			sed $(SED_OPTS) "s|YOUR_ADL_CLIENT_ID|$(ADL_CLIENT_ID)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			sed $(SED_OPTS) "s|YOUR_ADL_CREDENTIAL|$(ADL_CREDENTIAL)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
		fi; \
		if [ $(PROTOCOL) = gs ]; then \
			if [ ! -f /tmp/gsc-ci-service-account.key.json ]; then \
				echo "Google Service Account Key JSON file does exist in /tmp/gsc-ci-service-account.key.json"; \
				rm -rf $(PROTOCOL_HOME); \
				exit 1; \
			fi; \
			sed $(SED_OPTS) "s|YOUR_GOOGLE_STORAGE_KEYFILE|/tmp/gsc-ci-service-account.key.json|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
		fi; \
		echo "Created $(PROTOCOL) server configuration"; \
		if [ $(PROTOCOL) = wasbs ]; then \
			if [ -z "$(WASB_ACCOUNT_NAME)" ] || [ -z "$(WASB_ACCOUNT_KEY)" ]; then \
				echo "Azure Blob Storage Keys (WASB_ACCOUNT_NAME, WASB_ACCOUNT_KEY) not set"; \
				rm -rf $(PROTOCOL_HOME); \
				exit 1; \
			fi; \
			sed $(SED_OPTS) "s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_NAME|$(WASB_ACCOUNT_NAME)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
			sed $(SED_OPTS) "s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_KEY|$(WASB_ACCOUNT_KEY)|" $(PROTOCOL_HOME)/$(PROTOCOL)-site.xml; \
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
