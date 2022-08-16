# Concourse pipeline deployment
To facilitate pipeline maintenance, a Python utility 'deploy`
is used to generate the different pipelines for PXF master,
PXF 5x and release pipelines. It also allows the generation
of acceptance and custom pipelines for developers to use.

The utility uses the [Jinja2](http://jinja.pocoo.org/) template
engine for Python. This allows the generation of portions of the
pipeline from common blocks of pipeline code. Logic (Python code) can
be embedded to further manipulate the generated pipeline.

# Deploy the `pxf-build` (release) pipeline

To deploy the build pipeline for PXF, make sure PXF master branch is currently checked-out and run this command:

```shell script
make -C "${HOME}/workspace/pxf/concourse" build
```

# Deploy the `pxf-certification` (release) pipeline

To deploy the certifcation pipeline (forward compatibility) for PXF, make sure PXF master branch is currently checked-out and run this command:

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
of your development branch into `pxf-git-branch=master`. Also, make sure to change
the name of your development pipeline (i.e. `-p dev:<YOUR-PIPELINE>`).

50G Performance pipeline:

```shell script
make SCALE=50 -C "${HOME}/workspace/pxf/concourse" perf
```

500G Performance pipeline:

```shell script
make SCALE=500 -C "${HOME}/workspace/pxf/concourse" perf
```

# Deploy development PXF pipelines

The dev pipeline is an abbreviated version of the `pxf-build` pipeline.

To deploy dev pipeline against gpdb 5X_STABLE and 6X_STABLE branches, use:

```shell
make -C "${HOME}/workspace/pxf/concourse" dev
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

# Deploy `pg_regress` pipeline

This pipeline currently runs the smoke test group against the different clouds using `pg_regress` instead of automation.
It uses both external and foreign tables.
You can adjust the `folder-prefix`, `gpdb-git-branch`, `gpdb-git-remote`, `pxf-git-branch`, and `pxf-git-remote` in the makefile.

```shell
make -C "${HOME}/workspace/pxf/concourse" pg_regress
```

Expose the `pg_regress` pipeline:

```shell
fly -t ud expose-pipeline -p pg_regress
```
