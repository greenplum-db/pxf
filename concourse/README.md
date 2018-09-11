# Deploy pxf-docker-images pipeline
```
fly -t ud set-pipeline -p gpdb_pxf_docker-images \
    -c docker-images.yml -l ~/workspace/continuous-integration/secrets/gpdb-release-secrets.dev.yml
```

# Deploy production PXF pipelines
The following commands would create two PXF pipelines - one for **gpdb_master** and the other for **5X_STABLE**
```
fly -t ud set-pipeline -p dev:pxf_oss_master -c ./pxf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -l default.yml -v folder-prefix=prod/gpdb_branch -v test-env= \
    -l pxf-multinode-params.yml \
    -l ~/workspace/continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -v gpdb-branch=master -v icw_green_bucket=gpdb5-assert-concourse-builds
```

```
fly -t ud set-pipeline -p dev:pxf_oss_5X_STABLE -c ./pxf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_5X_STABLE-ci-secrets.yml \
    -l default.yml -v folder-prefix=prod/gpdb_branch -v test-env= \
    -l pxf-multinode-params.yml \
    -l ~/workspace/continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -v gpdb-branch=5X_STABLE -v icw_green_bucket=gpdb5-assert-concourse-builds
```

# Deploy the pull-request pipeline

```
fly -t ud set-pipeline -p dev:pxf_oss_pr \
    -c ./pxf_pr_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -l .pivotal-default.yml \
    -l pxf-multinode-params.yml \
    -l ~/workspace/continuous-integration/secrets/ccp_ci_secrets_ud.yml \
    -l ~/workspace/continuous-integration/secrets/pxf-secrets.yml \
    -v folder-prefix=dev/pivotal-default \
    -v test-env=dev \
    -v gpdb-branch=master \
    -v icw_green_bucket=gpdb5-assert-concourse-builds
```

# Deploy development PXF pipelines
Dev pipelines can be deployed with an optional feature name
```
./deploy dev master
Deploying dev:pxf_username pipeline with custom values from .username.yml ...
fly -t ud set-pipeline -p dev:pxf_username -c ./pxf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -l .username.yml -v prefix=dev/username
```
```
./deploy dev master feature-foo
Deploying dev:pxf_username-feature-foo pipeline with custom values from .username-feature-foo.yml ...
fly -t ud set-pipeline -p dev:pxf_username-feature-foo -c ./pxf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -l .username-feature-foo.yml -v prefix=dev/username-feature-foo
```
To deploy dev pipeline against gpdb 5X_STABLE branch, use:
```
./deploy dev 5x
Deploying dev:pxf_username pipeline with custom values from .username.yml ...
fly -t ud set-pipeline -p dev:pxf_username -c ./pxf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_5X_STABLE-ci-secrets.yml \
    -l .username.yml -v prefix=dev/username
```
```
./deploy dev 5x feature-foo
Deploying dev:pxf_username-feature-foo pipeline with custom values from .username-feature-foo.yml ...
fly -t ud set-pipeline -p dev:pxf_username-feature-foo -c ./pxf_pipeline.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_5X_STABLE-ci-secrets.yml \
    -l .username-feature-foo.yml -v prefix=dev/username-feature-foo
```

The master and 5X pipelines are exposed. Here are the commands to expose the pipelines, similar to the GPDB pipelines. The pipelines are currently located at https://ud.ci.gpdb.pivotal.io/
```
fly -t ud expose-pipeline -p gpdb_pxf_master
fly -t ud expose-pipeline -p gpdb_pxf_5X_STABLE
```

###Visual VM Debugging

To perform memory profiling add the following line to PXF's enviroment settings (`pxf/conf/pxf-env.sh`) on the machine where we want to debug:

```
export CATALINA_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote.port=9090 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=127.0.0.1"
```
