# `pxf cluster` CLI

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the root of this repository. This tool requires Go version 1.9 or higher. Follow the directions [here](https://golang.org/doc/) to get the language set up.

1. Go to the pxf-cli folder and install dependencies
   ```
   cd pxf/cli/go/src/pxf-cli
   go get github.com/onsi/ginkgo/ginkgo
   ```

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```
   This will put the binary at `pxf/cli/go/bin/pxf-cli`. You can also install the binary into `${PXF_HOME}/bin/pxf-cli` with:
   ```
   make install
   ```

1. There is also end to end testing for the pxf-cli located at `pxf/concourse/scripts/cli`. These tests can be run by flying the following pipeline:
```sh
MULTINODE=true make -C ~/workspace/pxf/concourse dev
```
To make iteration times faster, feel free to comment out the task `Test PXF-GP[[gp_ver]]-HDP2-SECURE-MULTI-IMPERS on RHEL7` in the `dev_build_pipeline-tpl.yml` file.

## Debugging the CLI on a live system

Because it's hard to mock out a Greenplum cluster, it's useful to debug on a real live cluster. We can do this using the [`delve`](https://github.com/go-delve/delve) project.

1. Install `dlv` command, see [here](https://github.com/go-delve/delve/blob/master/Documentation/installation/linux/install.md) for more details:

```bash
go get -u github.com/go-delve/delve/cmd/dlv
```

2. Optionally create a file with a list of commands, this could be useful to save you from re-running commands. You can actually run them with the `source` command in the `dlv` REPL:

```
config max-string-len 1000
break vendor/github.com/greenplum-db/gp-common-go-libs/cluster/cluster.go:351
continue
print commandList
```

3. Run the `dlv` command to enter the interactive REPL:

```bash
cd ~/workspace/pxf/cli/go/src/pxf-cli
GPHOME=/usr/local/greenplum-db dlv debug pxf-cli -- cluster restart
```

The [help page for dlv](https://github.com/go-delve/delve/tree/master/Documentation/cli) is useful.

## IDE Setup (GoLand)
* Start GoLand. Click "Open" and select the cli folder inside the pxf repo.
* Open preferences/settings and set under GO > GoPath, set Project GoPath to ~/workspace/pxf/cli/go or the equivalent.
* Under GO > Go Modules, select the enable button and leave the environment field empty.
* Check that it worked by running a test file (ex: go test pxf-cli/cmd)
