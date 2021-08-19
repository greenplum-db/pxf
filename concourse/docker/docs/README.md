# Docker container for PXF Documentation

PXF Documentation can be built using the `Dockerfile` provided in this
directory.

## How to build pxf-docs docker image locally?

```shell script
pushd ~/workspace/pxf/concourse/docker/docs/
docker build \
  --tag=pxf-docs \
  -f ~/workspace/pxf/concourse/docker/docs/Dockerfile \
  .
popd
```

## Run the pxf-docs image

```shell script
docker run -it \
  -p 9292:9292 \
  -p 1234:1234 \
  -v ~/workspace/pxf/docs:/pxfdocs \
  --workdir /pxfdocs/book \
  pxf-docs
```

Once inside the container run the following commands:

```shell script
bundle install
bundle exec bookbinder bind local
cd final_app
bundle install
bundle exec rackup --host=0.0.0.0
```

At this point, you can go to a browser window and type
http://localhost:9292 and you will see the formatted PXF open source
documentation.

NOTE:  If you perform multiple successive doc builds, remove the generated
Gemfile.lock and final_app/ and output/ directories before each build:

```shell script
cd ..
rm -rf Gemfile.lock final_app output
... build again ...
```
