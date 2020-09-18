PXF pg_regress Test Suite
===================================

This is a test suite that utilizes PostgreSQL's `pg_regress` testing framework to accomplish end-to-end testing of PXF.

It performs SQL queries via `psql` to 1) prepare data on an external source 2) create either external or foreign tables in Greenplum to query the data via PXF.
The `pg_regress` engine can then compare the results of these queries with expected results that we define to determine success or failure.

This is different from our automation framework (`pxf/automation`) which fires up a Java process that uses, e.g. HDFS APIs to prepare the data, then performs SQL queries for the rest.

Running the tests
===================================

## Pre-requisites

You need a running instance of Greenplum and PXF, along with a local installation of Greenplum (to be able to use the `pg_regress` framework).
The variables `PGHOST` and `PGPORT` must be pointing at the Greenplum master node, and Greenplum environment scripts like `${GPHOME}/greenplum_path.sh` and `gpdb/gpAux/gpdemo/gpdemo-env.sh` should be sourced.
`pg_config` must be on your path.

For data prep, the appropriate CLIs are required, as we shell out from SQL to these CLIs. These include `hdfs`, `hbase`, and `beeline`.
It is expected that these commands are configured correctly for communication with their respective services.

If your external data or Greenplum are remote, it is best to have password-less SSH configured, as some tests may use SCP to transfer prepared data files.

## Make targets

The tests are invoked by running the `make` command with certain environment variables set.

The targets available are either schedules (groups of tests that can be run in parallel) or a list of single tests.

For example, to run external table tests `HdfsSmokeTest` and `WritableSmokeTest` in serial:

```
make -C ~/workspace/pxf/regression HdfsSmokeTest WritableSmokeTest
```

To run the entire FDW smoke schedule in parallel:

```
make -C ~/workspace/pxf/regression fdw_smoke_schedule
```

## Environment variables

By setting environment variables you can change the location of the Greenplum master, the location of the data prep commands (`hdfs`, etc.), and even the cloud object store that will serve as HDFS proxy.

### General environment variables

All the general environment variables that come from `greenplum_path.sh` and
`gpdemo-env.sh` must be set. Additionally, `PXF_BASE` must be set if different
from `PXF_HOME`.

* `PXF_TEST_DEBUG`: set to anything to prevent deletion of data, and to run `pg_regress` in debug mode (optional)
* `SERVER_CONFIG`: the name of the config directory for your server, under `${PXF_BASE}/servers` (optional). If not set, PXF will use one of `${PXF_BASE}/servers/{default,s3,gs,adl,wasbs}` as appropriate
* `PGHOST`: the hostname of the master Greenplum node (optional). Needed when Greenplum is remote

### HCFS-specific variables

* `HCFS_CMD`: path to the `hdfs` CLI command. Required for HCFS tests
* `HCFS_PROTOCOL`: the protocol to use: `s3`, `gs`, `adl`, `wasbs`, `minio` (optional). Only needed when running against an object store
* `HCFS_BUCKET`: the location on the external object store (optional)

### HBase-specific variables

* `HBASE_CMD`: path to the `hbase` CLI command. Required for HBase tests

### Hive-specific variables

* `BEELINE_CMD`: path to Hive's `beeline` CLI command. Required for Hive tests
* `HIVE_HOST`: the hostname where Hive is running (optional). Needed if Hive is remote
* `HIVE_PRINCIPAL`: the Hive principal (optional). Needed when running against kerberized Hive

Writing tests
===================================

Tests may be added by creating SQL script templates under `sql` and corresponding expected response templates under `expected`.
The templates are interpolated by `scripts/substitute.bash` and corresponding files with a leading underscore are created.
These are the actual test files that `pg_regress` will use.
Results will be populated by `pg_regress` in a directory called `results`.
Test names should end in `Test` so that `make` considers them targets.
FDW test names should begin with the string `FDW_` so that the appropriate extension can be loaded by `pg_regress` (`pxf` vs. `pxf_fdw`).

Certain lines of test output can be ignored or mutated in `init_file` to make the tests pass consistently.

The basic directory structure after creating a test templates `sql/ExampleTest.sql`, `expected/ExampleTest.out`, and running `make ExampleTest` is as follows:

```
$ tree
.
├── Makefile
├── README.md
├── expected
│   ├── ExampleTest.out
│   └── _ExampleTest.out
├── init_file
├── regression.out
├── results
│   └── _ExampleTest.out
├── schedules
│   └── example_schedule
├── scripts
│   ├── cleanup_hcfs.bash
│   ├── generated
│   ├── hbase
│   │   ├── drop_small_data.rb
│   │   └── gen_small_data.rb
│   ├── hive
│   │   ├── cleanup_hive_smoke_test.sql
│   │   ├── create_hive_smoke_test_database.sql
│   │   └── load_small_data.sql
│   └── substitute.bash
└── sql
    ├── ExampleTest.sql
    └── _ExampleTest.sql
```

`regression.out` contains the output from the test report (success, failure, etc.).
If there was a test failure, you will also find `regression.diffs`, which shows why the test failed.
Note that under `sql` and `expected` there are files with leading underscores, they were generated by `scripts/substitute.bash`, then used to run the tests.
`results` contains the actual SQL commands and output from the test.

When creating a test, if adding new templated variables (words surrounded by double curly braces, e.g. `{{ VAR }}`), then the appropriate logic to interpolate should be added to `scripts/substitute.bash`.

Under `scripts` there are several other scripts which may be called from within test SQL code.
If they are specific to a certain type of test, they go under a separate directory, as in the case of `scripts/{hbase,hive}`.
To call any script under the `scripts` directory from the test, you can use: `{{ SCRIPT foo.sql }}`.
The substitution script will interpolate the full path to a uniquely-named script generated at `scripts/generated`, and any template variables in the script itself will also be interpolated.

### Creating Schedules

Schedules live under the `schedules` directory, and are a convenient way of making arbitrary groups of tests.

To make a schedule, make a new file, e.g. `schedules/example_schedule`.
Like tests, schedule files should end in `_schedule` to be considered as targets by `make`.
FDW schedules should begin with the string `fdw_` so that the `pxf_fdw` extension is loaded by `pg_regress`.

Inside the `schedules/example_schedule`, place some test names, preceded by `test: `:

```
test: _FirstParallelExampleTest _SecondParallelExampleTest
test: _LastExampleTest
```

The leading underscore is required here, because this should be the name of the test that `pg_regress` runs (after interpolating the test template).

In this case, when the following command is issued:

```
make -C ~/workspace/pxf/regression example_schedule
```

the tests `_FirstParallelExampleTest` and `_SecondParallelExampleTest` will run in parallel, followed by `_LastExampleTest`.
