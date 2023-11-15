# Concourse pipeline deployment
To facilitate pipeline maintenance, a Python utility 'deploy`
is used to generate the different pipelines for PXF main,
PXF 5x and release pipelines. It also allows the generation
of acceptance and custom pipelines for developers to use.

The utility uses the [Jinja2](http://jinja.pocoo.org/) template
engine for Python. This allows the generation of portions of the
pipeline from common blocks of pipeline code. Logic (Python code) can
be embedded to further manipulate the generated pipeline.

# Deploy the `pxf-build` (release) pipeline

To deploy the build pipeline for PXF, make sure PXF main branch is currently checked-out and run this command:

```shell script
make -C "${HOME}/workspace/pxf/concourse" build
```

# Deploy the `pxf-certification` (release) pipeline

To deploy the certifcation pipeline (forward compatibility) for PXF, make sure PXF main branch is currently checked-out and run this command:

```shell script
make -C "${HOME}/workspace/pxf/concourse" certification
```

# Deploy the singlecluster pipeline

The singlecluster pipeline generates the singlecluster tarball for CDH, HDP2,
and HDP3. The generated tarballs are then published to an S3 and GCS bucket.
The produced tarballs can then be consumed in the pxf-build pipelines.

```shell script
make -C "${HOME}/workspace/pxf/concourse" singlecluster
```

# Deploy the cloudbuild pipeline

```shell script
make -C "${HOME}/workspace/pxf/concourse" cloudbuild
```

# Deploy the pull-request pipeline

```shell script
make -C "${HOME}/workspace/pxf/concourse" pr
```

# Deploy the performance pipelines

10G Performance pipeline:

```shell script
make SCALE=10 -C "${HOME}/workspace/pxf/concourse" perf
```

You can deploy a development version of the perf pipeline by substituting the name
of your development branch into `pxf-git-branch=main`. Also, make sure to change
the name of your development pipeline (i.e. `-p dev:<YOUR-PIPELINE>`).

50G Performance pipeline:

```shell script
make SCALE=50 -C "${HOME}/workspace/pxf/concourse" perf
```

500G Performance pipeline:

```shell script
make SCALE=500 -C "${HOME}/workspace/pxf/concourse" perf
```

By default, these pipelines run perf on RHEL7.
If you would like to run pipelines using RHEL8, please include `REDHAT_MAJOR_VERSION=8` to the command.
Ex: `make SCALE=10 REDHAT_MAJOR_VERSION=8 -C "${HOME}/workspace/pxf/concourse" perf`

# Deploy development PXF release pipelines

The dev release pipeline performs most functions of the `pxf-build` release pipeline except for the tagging and bumping of the build version.

To deploy dev release pipeline, use:

```shell
make -C "${HOME}/workspace/pxf/concourse" dev-release
```

# Deploy development PXF pipelines

The dev pipeline is an abbreviated version of the `pxf-build` pipeline.

To deploy dev pipeline against gpdb 5X_STABLE and 6X_STABLE branches, use:

```shell
make -C "${HOME}/workspace/pxf/concourse" dev
```

To deploy multi-node dev pipeline, you can specify either the `MULTINODE`, `MULTINODE_EL8`, `MULTINODE_EL9` or
`MULTINODE_NO_IMPERSONATION`, which will also run CLI tests:

```shell
MULTINODE=true make -C "${HOME}/workspace/pxf/concourse" dev
```

This command will automatically point the pipeline at your currently checked-out branch of PXF.

# Deploy Longevity Testing PXF pipeline
The longevity testing pipeline is designed to work off a PXF tag that needs to be provided as a parameter when
creating the pipeline. The generated pipeline compiles PXF, creates a Greenplum CCP cluster and 2 secure dataproc clusters
and runs a multi-cluster security test every 15 minutes. CCP cluster is set with expiration time of more than 6 months, so
it needs to be cleaned manually and so do the dataproc clusters.

```shell
YOUR_TAG=<YOUR_TAG> make -C "${HOME}/workspace/pxf/concourse" longevity
```

## Uploading a new Apache Maven 3 version

The CI pipelines for PXF run automation tests using Apache Maven 3.x. Instead of downloading this directly from the Apache
mirrors or Apache archive, we store a copy in Google Cloud Storage to use when we create our images in Cloudbuild.
Typically, we will not be updating these values very often. However, if we need to upload a new version of Maven, you
can use a snippet like this one to download and then upload to GCS.

```bash
./scripts/download-maven-from-apache-mirror.sh <MAVEN-VERSION>
gcloud storage cp ../downloads/apache-maven-<MAVEN-VERSION>-bin.tar.gz gs://data-gpdb-ud-pxf-build-resources/apache-maven

# Example for Apache Maven 3.9.2
./scripts/download-spark-from-apache-mirror.sh 3.9.2
gcloud storage cp ../downloads/apache-maven-3.9.2-bin.tar.gz gs://data-gpdb-ud-pxf-build-resources/apache-maven

# Example for Apache Maven 3 Latest
$ ./scripts/download-spark-from-apache-mirror.sh latest
> Looking for latest maven-3 version...
> Latest maven version determined to be: 3.9.3
> Would you like to proceed (y/n)? y

gcloud storage cp ../downloads/apache-maven-3.9.3-bin.tar.gz gs://data-gpdb-ud-pxf-build-resources/apache-maven

```
