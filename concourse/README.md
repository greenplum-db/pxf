# Deploy pxf-docker-images pipeline
```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/docker-images.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb-release-secrets.dev.yml \
    -v pxf-git-remote=https://github.com/greenplum-db/pxf.git \
    -p gpdb_pxf_docker-images
```

# Deploy production PXF pipelines
The following commands would create two PXF pipelines - one for **gpdb_master** and the other for **5X_STABLE**
```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pxf_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/pxf-multinode-params.yml \
    -v folder-prefix=prod/gpdb_branch -v test-env= \
    -v gpdb-branch=master -v icw_green_bucket=gpdb5-assert-concourse-builds \
    -p pxf_master
```

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pxf_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/pxf-multinode-params.yml \
    -v folder-prefix=prod/gpdb_branch -v test-env= \
    -v gpdb-branch=5X_STABLE -v icw_green_bucket=gpdb5-stable-concourse-builds \
    -p pxf_5X_STABLE 
```

# Deploy the pull-request pipeline

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pxf_pr_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb-release-secrets.dev.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -v folder-prefix=dev/pivotal-default -v test-env=dev -v gpdb-branch=master \
    -v icw_green_bucket=gpdb5-assert-concourse-builds \
    -p pxf_pr
```

# Deploy the release pipeline

https://github.com/pivotal/gp-continuous-integration/blob/master/README.md#pxf_release
```
fly -t gpdb-prod set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/release_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_5X_STABLE-ci-secrets.yml \
    -l ~/workspace/pxf/concourse/pxf-multinode-params.yml \
    -l ~/workspace/gp-continuous-integration/secrets/pxf-release.prod.yml \
    -v test-env= -v gpdb-branch=5X_STABLE \
    -v folder-prefix=prod/gpdb_branch \
    -p pxf_release
```

# Deploy the performance pipelines

10G Performance pipeline:

```
fly -t ud set-pipeline \
    -c ~/workspace/pxf/concourse/pipelines/perf_pipeline.yml \
    -l ~/workspace/gp-continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/gp-continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/perf-settings.yml \
    -v gpdb-branch=master -v icw_green_bucket=gpdb5-assert-concourse-builds \
    -v pxf-git-branch=master -p pxf_perf
```

You can deploy a development version of the perf pipeline by substituting the name
of your development branch into `pxf-git-branch=master`. Also, make sure to change
the name of your development pipeline (i.e. `-p dev:<YOUR-PIPELINE>`).

50G Performance pipeline:

```
fly -t ud set-pipeline -c ~/workspace/pxf/concourse/perf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/perf-settings-50g.yml \
    -v gpdb-branch=master -v icw_green_bucket=gpdb5-assert-concourse-builds \
    -v pxf-git-branch=master -p pxf_perf-50g
```

500G Performance pipeline:

```
fly -t ud set-pipeline -c ~/workspace/pxf/concourse/perf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/pxf/concourse/perf-settings-500g.yml \
    -v gpdb-branch=master -v icw_green_bucket=gpdb5-assert-concourse-builds \
    -v pxf-git-branch=master -p pxf_perf-500g
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

### Visual VM Debugging

To perform memory profiling add the following line to PXF's environment settings (`pxf/conf/pxf-env.sh`) on the machine where we want to debug:

```
export CATALINA_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote.port=9090 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=127.0.0.1"
```
