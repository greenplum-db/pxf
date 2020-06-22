# set the concourse target default to dev
CONCOURSE ?= ud

# set the pxf-protocol-extension default branch to current branch
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

SHELL                        = /bin/bash
BUILD_PIPELINE_NAME         ?= pxf-build
DEV_BUILD_PIPELINE_NAME     ?= dev:$(USER)-$(BRANCH)
PR_BUILD_PIPELINE_NAME      ?= pxf_pr_pipeline
BUILD_DEB                   ?= false
CERTIFICATION_PIPELINE_NAME ?= pxf-certification
PIVNET_PIPELINE_NAME        ?= pivnet_artifacts
NUM_GPDB5_VERSIONS          ?= 10
NUM_GPDB6_VERSIONS          ?=  9
FLY_CMD                      = /usr/local/bin/fly
TEMPLATE_CMD                 = ./template_tool
FLY_OPTION_NON-INTERACTIVE   =
SECRETS_FILE                 = $(HOME)/workspace/gp-continuous-integration/secrets/pxf-secrets.yml
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

.PHONY: build certification dev pr
build: set-build-pipeline
certification: set-certification-pipeline
dev: set-dev-build-pipeline
pr: set-pr-build-pipeline

# ============================= BUILD PIPELINE TARGETS =============================
.PHONY: set-build-pipeline
set-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars \
		build_deb=$(BUILD_DEB) \
		slack_notification=$(SLACK) \
		git_branch=$(BRANCH) \
		num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) \
		num_gpdb6_versions=$(NUM_GPDB6_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(BUILD_PIPELINE_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(SECRETS_FILE) \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=folder-prefix=prod/gpdb_branch \
		--var=releng-release-bucket=pivotal-gpdb-concourse-resources-prod \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BUILD_PIPELINE_NAME}"

.PHONY: set-dev-build-pipeline
set-dev-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template dev_build_pipeline-tpl.yml --vars \
		build_deb=$(BUILD_DEB) \
		slack_notification=$(SLACK) \
		multinode=$(MULTINODE) \
		multinode_no_impersonation=$(MULTINODE_NO_IMPERSONATION) \
		jdk11=$(JDK11) \
		cdh=$(CDH) \
		hdp2=$(HDP2) \
		hdp3=$(HDP3) \
		mapr=$(MAPR) \
		ambari=$(AMBARI) \
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
		--load-vars-from=$(SECRETS_FILE) \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=folder-prefix=prod/gpdb_branch \
		--var=slack-integration-webhook-url=$${SLACK_INTEGRATION_WEBHOOK_URL} \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${DEV_BUILD_PIPELINE_NAME}"

.PHONY: set-pr-build-pipeline
set-pr-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template pr_pipeline-tpl.yml --vars \
		build_deb=$(BUILD_DEB) \
		slack_notification=false \
		pr_pipeline=true \
		num_gpdb5_versions=1 \
		num_gpdb6_versions=1 >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(PR_BUILD_PIPELINE_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(SECRETS_FILE) \
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
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(SECRETS_FILE) \
		--var=pxf-git-branch=${BRANCH} \
		${FLY_OPTION_NON-INTERACTIVE} || echo "pipelines/certification_pipeline.yml has errors"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${CERTIFICATION_PIPELINE_NAME}"

# ============================= HELPER PIPELINE TARGETS =============================

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
		--load-vars-from=$(SECRETS_FILE) \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--var pxf-git-branch=$(BRANCH) \
		--var pxf-git-remote=https://github.com/greenplum-db/pxf \
		--pipeline pivnet_artifacts \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${PIVNET_PIPELINE_NAME}"

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
