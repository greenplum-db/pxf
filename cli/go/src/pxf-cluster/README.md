# `pxf cluster` CLI

## Getting Started (on CentOS 6)

1. Install go
   ```
   yum install -y go
   ```

1. Add the go binaries path to the current path (you may want to add the following to your .bash_profile)
   ```
   export PATH="$PATH:/home/$(whoami)/go/bin"
   ```

1. Install the `dep` package manager and `ginkgo` test runner
   ```
   go get github.com/golang/dep/cmd/dep
   go get github.com/onsi/ginkgo/ginkgo
   ```

1. Go to the pxf-cluster folder and install dependencies
   ```
   cd pxf/cli/go/src/pxf-cluster
   make depend
   ```

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```

## Adding New Dependencies

1. Import the dependency in some source file (otherwise `dep` will refuse to install it)

2. Add the dependency to Gopkg.toml

3. Run `make depend`.