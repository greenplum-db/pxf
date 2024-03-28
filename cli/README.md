# `pxf cluster` CLI

## Getting Started

1. Ensure you are set up for PXF development by following the README.md at the root of this repository. This tool requires Go version 1.21 or higher. Follow the directions [here](https://golang.org/doc/) to get the language set up.

1. Run the tests
   ```
   make test
   ```

1. Build the CLI
   ```
   make
   ```
   This will put the binary `pxf-cli` into `pxf/cli/build/`. You can also install the binary into `${PXF_HOME}/bin/pxf-cli` with:
   ```
   make install
   ```
1. Run unit tests with Focused
   
   To run some specific unit tests, you can prepend an `F` to the `Describe` or `Context` to make them _Focused_, then run
   ```
   cd cmd
   ginkgo
   ```
   See more in [Ginkgo documentation](https://onsi.github.io/ginkgo/#focused-specs).

1. There is also end to end testing for the pxf-cli located at `pxf/concourse/scripts/cli`. These tests can be run by flying the following pipeline:

   ```sh
   # GP7_CLI=true brings in a standalone cli test test-pxf-gp7-cli-rocky8
   # MULTINODE_NO_IMPERSONATION=true brings in an embeded cli test in test-pxf-gp6-ext-hdp2-secure-multi-no-impers-centos7
   GP7_CLI=true MULTINODE_NO_IMPERSONATION=true make -C ~/workspace/pxf/concourse dev
   ```

## Debugging the CLI on a live system

Because it's hard to mock out a Greenplum cluster, it's useful to debug on a real live cluster. We can do this using the [`delve`](https://github.com/go-delve/delve) project.

1. Install `dlv` command, see [here](https://github.com/go-delve/delve/blob/master/Documentation/installation/linux/install.md) for more details:

   ```bash
   go install github.com/go-delve/delve/cmd/dlv@latest
   ```

1. Optionally create a file with a list of commands, this could be useful to save you from re-running commands. You can actually run them with the `source` command in the `dlv` REPL:

   ```
   config max-string-len 1000
   break main.main
   break cmd/cluster.go:24
   continue
   ```

   Inside the above `debug_command.txt`, a breakpoint is set in the main function, and another breakpoint inside the function `createCobraCommand`.
   The `continue` command on the third line will navigate you to the first breakpoint.

1. Run the `dlv` command to enter the interactive REPL:

   ```bash
   cd ~/workspace/pxf/cli
   GPHOME=/usr/local/greenplum-db dlv debug pxf-cli -- cluster restart
   ```

The [help page for dlv](https://github.com/go-delve/delve/tree/master/Documentation/cli) has more details about how to use `dlv` options.

## Debugging the unit tests

Run the `dlv` command to enter the interactive REPL, and set the breakpoints of the tests:

   ```bash
   cd ~/workspace/pxf/cli
   dlv test
   break cmd/cluster_test.go:613
   continue
   ```

## Testing on a multi-node cluster manually

To test cli with a real world Greenplum cluster, we can use the pipeline to spin up a multi-node cluster for testing.

1. In the pipeline, by default the GPDB cluster and the dataproc clusters are cleaned up after 3 hours.
If you want them to stay longer, change the value of `ccp_reap_minutes` inside `test-multinode-tpl.yml` to be a longer time.
Remember to delete the related VMs after the test so that we don't hva unnecessary long-standing VMs.
The names of these VMs begin with `ccp-`, and you can find the full names under job `generate-greenplum-cluster` and `generate-hadoop-cluster`.

1. Fly the pipeline with the following command:

   ```bash
   MULTINODE_NO_IMPERSONATION=true make -C ~/workspace/pxf/concourse dev
   ```
1. When `test-pxf-gp6-ext-hdp2-secure-multi-no-impers-centos7` job starts to run the task `test-pxf-gp6-ext-hdp2-secure-multi-no-impers-centos7`,
both Greenplum multi-node cluster and PXF have been set up. Use `fly i` command to log into the container of the test job.

1. If you want to test things on the coordinator, SSH into the coordinator host `cdw`. If you want to test things on segments, SSH into a segment host.

## IDE Setup (GoLand)
* Start GoLand. Click "Open" and select the cli folder inside the pxf repo.
* Open preferences/settings and set under GO > GOPATH, set Project GOPATH to ~/workspace/pxf/cli/go or the equivalent.
* Open preferences/settings and set under GO > GOROOT, set GOROOT to be the go version in the `go.mod`.
* Under GO > GO Modules, select the enable button and leave the environment field empty.
* Check that it worked by running a test file (ex: go test pxf-cli/cmd)
