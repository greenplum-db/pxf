# set the concourse target default to dev
CONCOURSE ?= ud

# set the pxf-protocol-extension default branch to current branch
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

SHELL                      = /bin/bash
BUILD_PIPELINE_5X_NAME    ?= pxf-build-GP5
BUILD_PIPELINE_6X_NAME    ?= pxf-build-GP6
FORCOM_PIPELINE_5X_NAME	  ?= pxf-forcom-GP5
FORCOM_PIPELINE_6X_NAME	  ?= pxf-forcom-GP6
PIVNET_PIPELINE_NAME      ?= pivnet_artifacts
NUM_GPDB5_VERSIONS        ?= 10
NUM_GPDB6_VERSIONS        ?=  9
FLY_CMD                    = /usr/local/bin/fly
TEMPLATE_CMD               = ./template_tool
FLY_OPTION_NON-INTERACTIVE =

.PHONY: build-5 build-6 forcom-5 forcom-6
build-5: set-5X-build-pipeline
build-6: set-6X-build-pipeline
forcom-5: set-5X-forward-compatibility-pipeline
forcom-6: set-6X-forward-compatibility-pipeline

# ============================= BUILD PIPELINE TARGETS =============================
.PHONY: set-5X-build-pipeline
set-5X-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars gp_ver=5 num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(BUILD_PIPELINE_5X_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/pxf-secrets.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=folder-prefix=prod/gpdb_branch \
		--var=gpdb-branch=5X_STABLE \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BUILD_PIPELINE_5X_NAME}"

.PHONY: set-6X-build-pipeline
set-6X-build-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars gp_ver=6 num_gpdb6_versions=$(NUM_GPDB6_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(BUILD_PIPELINE_6X_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/pxf-secrets.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb-6X_STABLE-release-secrets.prod.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=folder-prefix=prod/gpdb_branch \
		--var=gpdb-branch=6X_STABLE \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BUILD_PIPELINE_6X_NAME}"

# ============================= FORWARD COMPATIBILITY PIPELINE TARGETS =============================

.PHONY: set-5X-forward-compatibility-pipeline
set-5X-forward-compatibility-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template forcom_pipeline-tpl.yml --vars gp_ver=5 >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(FORCOM_PIPELINE_5X_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/pxf-secrets.yml \
		--var=pxf-git-branch=${BRANCH} \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${FORCOM_PIPELINE_5X_NAME}"

.PHONY: set-6X-forward-compatibility-pipeline
set-6X-forward-compatibility-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template forcom_pipeline-tpl.yml --vars gp_ver=6 >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(FORCOM_PIPELINE_6X_NAME) \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/pxf-secrets.yml \
		--var=pxf-git-branch=${BRANCH} \
		${FLY_OPTION_NON-INTERACTIVE} || echo "Generated yaml has errors: check $${PIPELINE_FILE}"

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${FORCOM_PIPELINE_6X_NAME}"

# ============================= HELPER PIPELINE TARGETS =============================

.PHONY: set-pivnet
set-pivnet: set-pivnet-pipeline

.PHONY: set-pivnet-pipeline
set-pivnet-pipeline:
	@PIPELINE_FILE=$$(mktemp) && \
	$(TEMPLATE_CMD) --template get_pivnet_artifacts-tpl.yml --vars num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) num_gpdb6_versions=$(NUM_GPDB6_VERSIONS) >"$${PIPELINE_FILE}" && \
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config "$${PIPELINE_FILE}" \
		--load-vars-from $(HOME)/workspace/gp-continuous-integration/secrets/pxf-secrets.yml \
		--load-vars-from $(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--var pxf-git-branch=external-table \
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
