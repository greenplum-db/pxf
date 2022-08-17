# set the concourse target default to dev
CONCOURSE ?= ud

# set the pxf-protocol-extension default branch to current branch
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

SHELL                        = /bin/bash
BUILD_PIPELINE_NAME         ?= pxf-5-build
DEV_BUILD_PIPELINE_NAME     ?= dev:$(USER)-$(BRANCH)
PR_BUILD_PIPELINE_NAME      ?= pxf_5_pr_pipeline
CERTIFICATION_PIPELINE_NAME ?= pxf-certification
PERF_PIPELINE_NAME          ?= pxf_perf-$(SCALE)g
PIVNET_PIPELINE_NAME        ?= pivnet_artifacts
NUM_GPDB5_VERSIONS          ?= 10
NUM_GPDB6_VERSIONS          ?=  9
FLY_CMD                      = /usr/local/bin/fly
TEMPLATE_CMD                 = ./template_tool
FLY_OPTION_NON-INTERACTIVE   =
EMAIL                       ?= true
SLACK                       ?= true
# below are dev-build specific vars
MULTINODE                   ?= false
MULTINODE_NO_IMPERSONATION  ?= false
JDK11                       ?= false
CDH                         ?= false
HDP2                        ?= false
HDP3                        ?= false
MAPR                        ?= false
AMBARI                      ?= false
S3                          ?= false
ADL                         ?= false
GS                          ?= false
MINIO                       ?= false
OEL7                        ?= false
FILE                        ?= false

.PHONY: build certification dev pr cloudbuild longevity pg_regress
build: set-build-pipeline
certification: set-certification-pipeline
dev-release: set-dev-release-pipeline
dev: set-dev-build-pipeline
pr: set-pr-build-pipeline
cloudbuild: set-cloudbuild-pipeline
longevity: set-longevity-pipeline
pg_regress: set-pg_regress-pipeline

# ============================= BUILD PIPELINE TARGETS =============================
.PHONY: set-build-pipeline
set-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars \
		environment=prod \
		email_notification=$(EMAIL) \
		slack_notification=$(SLACK) \
		num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) \
		num_gpdb6_versions=$(NUM_GPDB6_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(BUILD_PIPELINE_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--var=pxf-git-branch=branch-5.16 \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BUILD_PIPELINE_NAME}"

.PHONY: set-dev-release-pipeline
set-dev-release-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars \
		environment=dev \
		email_notification=false \
		slack_notification=false \
		git_branch=$(BRANCH) \
		num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) \
		num_gpdb6_versions=$(NUM_GPDB6_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(DEV_BUILD_PIPELINE_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=pxf-build-bucket-prefix=dev/$(USER)-$(BRANCH) \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${DEV_BUILD_PIPELINE_NAME}"

.PHONY: set-dev-build-pipeline
set-dev-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template dev_build_pipeline-tpl.yml --vars \
		slack_notification=$(SLACK) \
		multinode=$(MULTINODE) \
		multinode_no_impersonation=$(MULTINODE_NO_IMPERSONATION) \
		jdk11=$(JDK11) \
		cdh=$(CDH) \
		hdp2=$(HDP2) \
		hdp3=$(HDP3) \
		mapr=$(MAPR) \
		ambari=$(AMBARI) \
		file=$(FILE) \
		s3=$(S3) \
		adl=$(ADL) \
		gs=$(GS) \
		minio=$(MINIO) \
		oel7=$(OEL7) \
		dev_pipeline=true \
		user=$(USER) \
		branch=$(BRANCH) \
		num_gpdb5_versions=1 \
		num_gpdb6_versions=1 >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(DEV_BUILD_PIPELINE_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=slack-integration-webhook-url=$${SLACK_INTEGRATION_WEBHOOK_URL} \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${DEV_BUILD_PIPELINE_NAME}"

.PHONY: set-pr-build-pipeline
set-pr-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template pr_pipeline-tpl.yml --vars \
		slack_notification=false \
		pr_pipeline=true \
		num_gpdb5_versions=1 \
		num_gpdb6_versions=1 >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(PR_BUILD_PIPELINE_NAME) \
		--config "$${PIPELINE_FILE}" \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${PR_BUILD_PIPELINE_NAME}"


# ============================= FORWARD COMPATIBILITY PIPELINE TARGETS =============================

.PHONY: set-certification-pipeline
set-certification-pipeline:
	@$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(CERTIFICATION_PIPELINE_NAME) \
		--config pipelines/certification_pipeline.yml \
		--var=pxf-git-branch=${BRANCH} \
		${FLY_OPTION_NON-INTERACTIVE} || echo "pipelines/certification_pipeline.yml has errors"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${CERTIFICATION_PIPELINE_NAME}"

# ============================= HELPER PIPELINE TARGETS =============================

.PHONY: set-cloudbuild-pipeline
set-cloudbuild-pipeline:
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config $(HOME)/workspace/pxf/concourse/pipelines/cloudbuild_pipeline.yml \
		--var pxf-git-branch=$(BRANCH) \
		--pipeline cloudbuild

.PHONY: pivnet
pivnet: set-pivnet-pipeline

.PHONY: set-pivnet-pipeline
set-pivnet-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template get_pivnet_artifacts-tpl.yml --vars \
		num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) \
		num_gpdb6_versions=$(NUM_GPDB6_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config "$${PIPELINE_FILE}" \
		--var pxf-git-branch=$(BRANCH) \
		--pipeline pivnet_artifacts \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${PIVNET_PIPELINE_NAME}"

.PHONY: perf
perf:
	@if [ -z '$(SCALE)' ]; then \
		echo 'Specify the SCALE for the test (i.e make SCALE=10 perf). Allowed SCALE values are 10, 50, 500'; \
		exit 1; \
	fi
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config $(HOME)/workspace/pxf/concourse/pipelines/perf_pipeline.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/perf-settings-$(SCALE)g.yml \
		--var pxf-git-branch=$(BRANCH) \
		--pipeline $(PERF_PIPELINE_NAME) \
		${FLY_OPTION_NON-INTERACTIVE}

.PHONY: singlecluster
singlecluster:
	fly -t ud set-pipeline \
		-c ~/workspace/pxf/concourse/pipelines/singlecluster-pipeline.yml \
		-v pxf-git-branch=master -p pxf-singlecluster

# ============================= LONGEVITY PIPELINE TARGETS =============================
.PHONY: set-longevity-pipeline
set-longevity-pipeline:
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=dev:longevity_$(YOUR_TAG)_6X_STABLE \
		--config pipelines/longevity_pipeline.yml \
		--load-vars-from=settings/pxf-multinode-params.yml \
		--var=folder-prefix=dev/pivotal \
		--var=test-env=dev \
		--var=icw_green_bucket=gpdb5-assert-concourse-builds \
		--var=gcs-bucket-intermediates=pivotal-gpdb-concourse-resources-intermediates-prod \
		--var=gcs-bucket-resources-prod=pivotal-gpdb-concourse-resources-prod \
		--var=gpdb-branch=6X_STABLE \
		--var=pgport=6000 \
		--var=pxf-tag=$(YOUR_TAG)


# ============================= PG_REGRESS PIPELINE TARGETS =============================
.PHONY: set-pg_regress-pipeline
set-pg_regress-pipeline:
	$(FLY_CMD) --target=$(CONCOURSE) \
	set-pipeline \
	--check-creds \
	--pipeline=pg_regress \
	--config pipelines/pg_regress_pipeline.yml \
	 -v "folder-prefix=dev/${USER}" -v gpdb-branch=master -v pgport=7000 \
	 -v gpdb-git-branch=master \
	 -v pxf-git-branch=master
## ----------------------------------------------------------------------
## List explicit rules
## ----------------------------------------------------------------------

.PHONY: list
list:
	@sh -c "$(MAKE) -p no_targets__ 2>/dev/null | \
	awk -F':' '/^[a-zA-Z0-9][^\$$#\/\\t=]*:([^=]|$$)/ {split(\$$1,A,/ /);for(i in A)print A[i]}' | \
	grep -v Makefile | \
	grep -v '%' | \
	grep -v '__\$$' | \
	sort"

## ----------------------------------------------------------------------
## Lint targets
## ----------------------------------------------------------------------
.PHONY: check
check:
	$(MAKE) lint

.PHONY: lint
lint:
	$(MAKE) shfmt shellcheck yamllint

.PHONY: shfmt
shfmt:
	docker run --rm -v ${CURDIR}:/code mvdan/shfmt:v2.6.4 -d /code

.PHONY: shellcheck
shellcheck:
	docker run --rm -v ${CURDIR}:/code mvdan/shfmt:v2.6.4 -f /code | xargs docker run --rm -v ${CURDIR}:/code koalaman/shellcheck:v0.7.0 -e SC1090,SC1091

.PHONY: yamllint
yamllint:
	docker run --rm -v ${CURDIR}:/code cytopia/yamllint /code -c /code/.yamllint
