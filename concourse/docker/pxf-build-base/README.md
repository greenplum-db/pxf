# How to generate a tarball with PXF dependencies locally

You can generate the tarball with all the dependencies for PXF (`.m2` cache,
`.gradle` cache, `.tomcat` directory, go dependencies).

### Requirements

- PXF source code (assume that the source code lives in `~/workspace/pxf`)

To generate the tarball run the following commands:

```shell script

mkdir -p ~/workspace/build
cd ~/workspace/pxf

echo $(git rev-parse HEAD) > pxf_commit_sha

docker build --tag=pxf-build-dev \
  -f ~/workspace/pxf/concourse/docker/pxf-build-base/Dockerfile .

docker run --rm \
  -v ~/workspace/build:/tmp/build/ \
  pxf-build-dev /bin/bash -c "cp /tmp/pxf-build-dependencies.tar.gz /tmp/build/"

```

Alternatively, you can run the build using Google Cloud Build by doing the
following:

```shell script

cd ~/workspace/pxf

echo $(git rev-parse HEAD) > pxf_commit_sha

gcloud builds submit . --config=cloudbuild.yaml \
  --substitutions=_PXF_BUILD_BUCKET=<YOUR_BUCKET_NAME>

```

The dependencies will be stored in Google Cloud Storage in the
`gs://${_PXF_BUILD_BUCKET}/build-dependencies/pxf-build-dependencies.tar.gz`
path.