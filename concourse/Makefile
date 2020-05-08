# set the concourse target default to dev
CONCOURSE ?= ud

# set the pxf-protocol-extension default branch to current branch
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

SHELL                      = /bin/bash
BUILD_PIPELINE_NAME       ?= pxf-build-GP6
BC_5X_PIPELINE_NAME       ?= pgregress_external_table_5X_STABLE
BC_6X_PIPELINE_NAME       ?= pgregress_external_table_6X_STABLE
PIVNET_PIPELINE_NAME      ?= pivnet_artifacts
NUM_GPDB5_VERSIONS        ?= 10
NUM_GPDB6_VERSIONS        ?=  9
FLY_CMD                    = /usr/local/bin/fly
TEMPLATE_CMD               = ./template_tool
FLY_OPTION_NON-INTERACTIVE =

.PHONY: build-5 build-6
build-5: set-5X-build-pipeline
build-6: set-6X-build-pipeline

.PHONY: set-5X-build-pipeline
set-5X-build-pipeline:
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(BUILD_PIPELINE_NAME) \
		--config=<($(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars gp_ver=5 num_gpdb5_versions=$(NUM_GPDB5_VERSIONS)) \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=folder-prefix=prod/gpdb_branch \
		--var=gpdb-branch=5X_STABLE \
		--var=ccp-git-branch=fix-rpm-install \
		${FLY_OPTION_NON-INTERACTIVE}

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BUILD_PIPELINE_NAME}"

.PHONY: set-6X-build-pipeline
set-6X-build-pipeline:
	$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--pipeline=$(BUILD_PIPELINE_NAME) \
		--config=<($(TEMPLATE_CMD) --template build_pipeline-tpl.yml --vars gp_ver=6 num_gpdb6_versions=$(NUM_GPDB6_VERSIONS)) \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb-6X_STABLE-release-secrets.prod.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--load-vars-from=$(HOME)/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
		--load-vars-from=$(HOME)/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
		--var=pxf-git-branch=${BRANCH} \
		--var=folder-prefix=prod/gpdb_branch \
		--var=gpdb-branch=6X_STABLE \
		--var=ccp-git-branch=fix-rpm-install \
		${FLY_OPTION_NON-INTERACTIVE}

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BUILD_PIPELINE_NAME}"

.PHONY: set-BC-5X
set-BC-5X: set-BC-5X-pipeline

.PHONY: set-BC-5X-pipeline
set-BC-5X-pipeline:
	@$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config <($(TEMPLATE_CMD) --template external-table-tpl.yml --vars gpdb_type=5X_STABLE num_gpdb5_versions=$(NUM_GPDB5_VERSIONS)) \
		--load-vars-from $(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--var pxf-git-branch=external-table \
		--var pxf-git-remote=https://github.com/greenplum-db/pxf \
		--var folder-prefix=dev/oalbertini-external-table \
		--var gpdb-branch=5X_STABLE \
		--pipeline $(BC_5X_PIPELINE_NAME) \
		${FLY_OPTION_NON-INTERACTIVE}

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BC_5X_PIPELINE_NAME}"

.PHONY: set-BC-6X
set-BC-6X: set-BC-6X-pipeline

.PHONY: set-BC-6X-pipeline
set-BC-6X-pipeline:
	@$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config <($(TEMPLATE_CMD) --template external-table-tpl.yml --vars gpdb_type=6X_STABLE num_gpdb6_versions=$(NUM_GPDB6_VERSIONS)) \
		--load-vars-from $(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--var pxf-git-branch=external-table \
		--var pxf-git-remote=https://github.com/greenplum-db/pxf \
		--var folder-prefix=dev/oalbertini-external-table \
		--var gpdb-branch=6X_STABLE \
		--pipeline $(BC_6X_PIPELINE_NAME) \
		${FLY_OPTION_NON-INTERACTIVE}

	@echo using the following command to unpause the pipeline:
	@echo -e "\t$(FLY_CMD) -t ${CONCOURSE} unpause-pipeline --pipeline ${BC_6X_PIPELINE_NAME}"

.PHONY: set-pivnet
set-pivnet: set-pivnet-pipeline

.PHONY: set-pivnet-pipeline
set-pivnet-pipeline:
	@$(FLY_CMD) --target=$(CONCOURSE) \
		set-pipeline \
		--check-creds \
		--config <($(TEMPLATE_CMD) --template get_pivnet_artifacts-tpl.yml --vars num_gpdb5_versions=$(NUM_GPDB5_VERSIONS) num_gpdb6_versions=$(NUM_GPDB6_VERSIONS)) \
		--load-vars-from $(HOME)/workspace/gp-continuous-integration/secrets/pxf-pivnet-secrets.yml \
		--load-vars-from $(HOME)/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
		--var pxf-git-branch=external-table \
		--var pxf-git-remote=https://github.com/greenplum-db/pxf \
		--pipeline pivnet_artifacts
		${FLY_OPTION_NON-INTERACTIVE}

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
