# Concourse pipeline deployment
To facilitate pipeline maintenance, a Python utility 'deploy`
is used to generate the different pipelines for PXF master,
PXF 5x and release pipelines. It also allows the generation
of acceptance and custom pipelines for developers to use.

The utility uses the [Jinja2](http://jinja.pocoo.org/) template
engine for Python. This allows the generation of portions of the
pipeline from common blocks of pipeline code. Logic (Python code) can
be embedded to further manipulate the generated pipeline.

# Deploy pxf-docker-images pipeline
```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/docker-images.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb-release-secrets.dev.yml \
    -v pxf-git-branch=master -p gpdb_pxf_docker-images
```

# Deploy production PXF pipelines
The following commands would create three PXF pipelines - **gpdb_master**, **6X_STABLE** and **5X_STABLE**
```
pushd ~/workspace/gp-continuous-integration && git pull && popd
./deploy prod master
./deploy prod 6x
./deploy prod 5x
```

The following commands will expose these pipelines:
```
fly -t ud expose-pipeline -p pxf_master
fly -t ud expose-pipeline -p pxf_6X_STABLE
fly -t ud expose-pipeline -p pxf_5X_STABLE
```

# Deploy the pull-request pipeline

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/pxf_pr_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb-release-secrets.dev.yml \
    -p pxf_pr
```

# Deploy the release pipeline

https://github.com/pivotal/gp-continuous-integration/blob/master/README.md#pxf_release
```
./deploy prod 5x -p release
./deploy prod 6x -p release
```

# Deploy the performance pipelines

10G Performance pipeline:

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/perf_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/settings/perf-settings-10g.yml \
    -v gpdb-branch=5X_STABLE -v icw_green_bucket=gpdb5-stable-concourse-builds \
    -v pxf-git-branch=master -p pxf_perf-10g
```

You can deploy a development version of the perf pipeline by substituting the name
of your development branch into `pxf-git-branch=master`. Also, make sure to change
the name of your development pipeline (i.e. `-p dev:<YOUR-PIPELINE>`).

50G Performance pipeline:

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/perf_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/settings/perf-settings-50g.yml \
    -v gpdb-branch=5X_STABLE -v icw_green_bucket=gpdb5-stable-concourse-builds \
    -v pxf-git-branch=master -p pxf_perf-50g
```

500G Performance pipeline:

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/perf_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/settings/perf-settings-500g.yml \
    -v gpdb-branch=5X_STABLE -v icw_green_bucket=gpdb5-stable-concourse-builds \
    -v pxf-git-branch=master -p pxf_perf-500g
```

# Deploy a PXF acceptance pipeline
Acceptance pipelines can be deployed for feature testing purposes.
```
./deploy dev master -a -n acceptance
```
For 5x:
```
./deploy dev 5x -a -n acceptance
```
After acceptance, the pipeline can be cleaned up as follows:
```
fly -t ud dp -p acceptance
```

# Deploy development PXF pipelines
Dev pipelines can be deployed with an optional feature name
```
./deploy dev master
```

```
./deploy dev master feature-foo
```
To deploy dev pipeline against gpdb 5X_STABLE branch, use:
```
./deploy dev 5x
```
```
./deploy dev 5x feature-foo
```

The master and 5X pipelines are exposed. Here are the commands to expose the pipelines, similar to the GPDB pipelines. The pipelines are currently located at https://ud.ci.gpdb.pivotal.io/
```
fly -t ud expose-pipeline -p pxf_master
fly -t ud expose-pipeline -p pxf_5X_STABLE
```

# Deploy Longevity Testing PXF pipeline
The longevity testing pipeline is designed to work off a PXF tag that needs to be provided as a parameter when
creating the pipeline. The generated pipeline compiles PXF, creates a Greenplum CCP cluster and 2 secure dataproc clusters
and runs a multi-cluster security test every 15 minutes. CCP cluster is set with expiration time of more than 6 months, so
it needs to be cleaned manually and so do the dataproc clusters.

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/longevity_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/pxf/concourse/settings/pxf-multinode-params.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud_kerberos.yml \
    -v folder-prefix=dev/pivotal -v test-env=dev \
    -v icw_green_bucket=gpdb5-assert-concourse-builds \
    -v gcs-bucket-intermediates=pivotal-gpdb-concourse-resources-intermediates-prod \
    -v gcs-bucket-resources-prod=pivotal-gpdb-concourse-resources-prod \
    -v gpdb-branch=6X_STABLE -v pgport=6000 \
    -v pxf-tag=<YOUR-TAG> -p dev:longevity_<YOUR-TAG>_6X_STABLE
```

# Deploy `pg_regress` pipeline

This pipeline currently runs the smoke test group against the different clouds using `pg_regress` instead of automation.
It uses both external and foreign tables.
You can adjust the `folder-prefix`, `gpdb-git-branch`, `gpdb-git-remote`, `pxf-git-branch`, and `pxf-git-remote`.
For example, you may want to work off of a development branch for PXF or Greenplum.

```
fly -t ud set-pipeline -p pg_regress \
	-c ~/workspace/pxf/concourse/pipelines/pg_regress_pipeline.yml \
	-l ~/workspace/gp-continuous-integration/secrets/gpdb6-integration-testing.dev.yml \
	-l ~/workspace/gp-continuous-integration/secrets/ccp-integration-pipelne-secrets.yml \
	-l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
	-v folder-prefix=dev/pivotal \
	-v gpdb-git-branch=pxf-fdw-pass-filter-string -v gpdb-git-remote=https://github.com/pivotal/gp-gpdb-dev \
	-v pxf-git-branch=master -v pxf-git-remote=https://github.com/greenplum-db/pxf \
```
