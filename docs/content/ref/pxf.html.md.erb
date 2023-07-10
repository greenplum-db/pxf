---
title: pxf
---

Manage the PXF configuration and the PXF Service instance on the local Greenplum Database host.

## <a id="topic1__section2"></a>Synopsis

``` pre
pxf <command> [<option>]
```

where \<command\> is:

``` pre
cluster
help
init (deprecated)
migrate
prepare
register
reset (deprecated)
restart
start
status
stop
sync
version
```

## <a id="topic1__section3"></a>Description

The `pxf` utility manages the PXF configuration and the PXF Service instance on the local Greenplum Database host. You can use the utility to:

- Synchronize the PXF configuration from the coordinator host to the standby coordinator host or to a segment host.
- Start, stop, or restart the PXF Service instance on the coordinator host, standby coordinator host, or a specific segment host, or display the status of the PXF Service instance running on the coordinator, standby coordinator, or a segment host.
- Copy the PXF extension control file from a PXF installation on the host to the Greenplum installation on the host after a Greenplum upgrade.
- Prepare a new `$PXF_BASE` runtime configuration directory on the host.

(Use the [`pxf cluster`](pxf-cluster.html#topic1) command to prepare a new `$PXF_BASE` on all hosts, copy the PXF extension control file to `$GPHOME` on all hosts, synchronize the PXF configuration to the Greenplum Database cluster, or to start, stop, or display the status of the PXF Service instance on all hosts in the cluster.)

## <a id="commands"></a>Commands

<dt>cluster</dt>
<dd>Manage the PXF configuration and the PXF Service instance on all Greenplum Database hosts. See <code>pxf cluster</code>.</dd>

<dt>help</dt>
<dd>Display the <code>pxf</code> management utility help message and then exit.</dd>

<dt>init (deprecated)</dt>
<dd>The command is equivalent to the <code>register</code> command.</dd>

<dt>migrate</dt>
<dd>Migrate the configuration in a PXF 5 <code>$PXF_CONF</code> directory to <code>$PXF_BASE</code> on the host. When you run the command, you must identify the PXF 5 configuration directory via an environment variable named <code>PXF_CONF</code>. PXF migrates the version 5 configuration to the current <code>$PXF_BASE</code>, copying and merging files and directories as necessary. <div class="note"><b>Note:</b> You must manually migrate any <code>pxf-log4j.properties</code> customizations to the <code>pxf-log4j2.xml</code> file.</div></dd>

<dt>prepare</dt>
<dd>Prepare a new <code>$PXF_BASE</code> directory on the host. When you run the command, you must identify the new PXF runtime configuration directory via an environment variable named <code>PXF_BASE</code>. PXF copies runtime configuration file templates and directories to this <code>$PXF_BASE</code>.</dd>

<dt>register</dt>
<dd>Copy the PXF extension files from the PXF installation on the host to the Greenplum installation on the host. This command requires that <code>$GPHOME</code> be set, and is run once after you install PXF 6.x the first time, or run when you upgrade your Greenplum Database installation.</dd>

<dt>reset (deprecated)</dt>
<dd>The command is a no-op.</dd>

<dt>restart</dt>
<dd>Restart the PXF Service instance running on the local coordinator host, standby coordinator host, or segment host.</dd>

<dt>start</dt>
<dd>Start the PXF Service instance on the local coordinator host, standby coordinator host, or segment host.</dd>

<dt>status</dt>
<dd>Display the status of the PXF Service instance running on the local coordinator host, standby coordinator host, or segment host.</dd>

<dt>stop  </dt>
<dd>Stop the PXF Service instance running on the local coordinator host, standby coordinator host, or segment host.</dd>

<dt>sync  </dt>
<dd>Synchronize the PXF configuration (<code>$PXF_BASE</code>) from the coordinator host to a specific Greenplum Database standby coordinator host or segment host. You must run <code>pxf sync</code> on the coordinator host. By default, this command updates files on and copies files to the remote. You can instruct PXF to also delete files during the synchronization; see Options below.</dd>

<dt>version  </dt>
<dd>Display the PXF version and then exit.</dd>

## <a id="options"></a>Options

The `pxf sync` command, which you must run on the Greenplum Database coordinator host, takes the following option and argument:

<dt>&#8211;d | &#8211;&#8211;delete </dt>
<dd>Delete any files in the PXF user configuration on <code>&lt;gphost></code> that are not also present on the coordinator host. If you specify this option, you must provide it on the command line before <code>&lt;gphost></code>.</dd>

<dt>&lt;gphost> </dt>
<dd>The Greenplum Database host to which to synchronize the PXF configuration. Required. <code>&lt;gphost></code> must identify the standby coordinator host or a segment host.</dd>

## <a id="topic1__section5"></a>Examples

Start the PXF Service instance on the local Greenplum host:

``` shell
$ pxf start
```

## <a id="topic1__section6"></a>See Also

[`pxf cluster`](pxf-cluster.html#topic1)
