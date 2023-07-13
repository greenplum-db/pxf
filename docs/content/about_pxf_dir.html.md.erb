---
title: About the Installation and Configuration Directories
---

This documentation uses `<PXF_INSTALL_DIR>` to refer to the PXF installation directory. Its value depends on how you have installed PXF:

 - If you installed PXF as part of Greenplum Database, its value is `$GPHOME/pxf`.
 - If you installed the PXF `rpm` or `deb` package, its value is `/usr/local/pxf-gp<greenplum-major-version>`, or the directory of your choosing (CentOS/RHEL only). 

`<PXF_INSTALL_DIR>` includes both the PXF executables and the PXF runtime configuration files and directories. In PXF 5.x, you needed to specify a `$PXF_CONF` directory for the runtime configuration when you initialized PXF. In PXF 6.x, however, no initialization is required: `$PXF_BASE` now identifies the runtime configuration directory, and the default `$PXF_BASE` is `<PXF_INSTALL_DIR>`.

If you want to store your configuration and runtime files in a different location, see [Relocating $PXF_BASE](#movebase).

<div class="note"><b>Note:</b> This documentation uses <code>&lt;PXF_INSTALL_DIR></code> to reference the PXF installation directory. This documentation uses the <code>$PXF_BASE</code> environment variable to reference the PXF runtime configuration directory. PXF uses the variable internally. It only needs to be set in your shell environment if you explicitly relocate the directory.</div>

## <a id="installed"></a>PXF Installation Directories

The following PXF files and directories are installed to `<PXF_INSTALL_DIR>` when you install Greenplum Database or the PXF 6.x `rpm` or `deb` package:

| Directory | Description                                                                                                   |
|--------------------------------|------------------------------------------------------------------------------------------|
| application/                | The PXF Server application JAR file.   |
| bin/                | The PXF command line executable directory.   |
| commit.sha          | The commit identifier for this PXF release.   |
| gpextable/           | The PXF extension files. PXF copies the `pxf.control` file from this directory to the Greenplum installation (`$GPHOME`) on a single host when you run the `pxf register` command, or on all hosts in the cluster when you run the `pxf [cluster] register` command from the Greenplum coordinator host.   |
| share/                | The directory for shared PXF files that you may require depending on the external data stores that you access. `share/` initially includes only the PXF HBase JAR file. |
| templates/  | The PXF directory for server configuration file templates. |
| version              | The PXF version.             |

The following PXF directories are installed to `$PXF_BASE` when you install Greenplum Database or the PXF 6.x `rpm` or `deb` package:

| Directory | Description                                                                                                                                                                                                                                |
|--------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| conf/                | The location of [user-customizable PXF configuration files](config_files.html) for PXF runtime and logging configuration settings. This directory contains the `pxf-application.properties`, `pxf-env.sh`, `pxf-log4j2.xml`, and `pxf-profiles.xml` files. |
| keytabs/  | The default location of the PXF Service Kerberos principal keytab file.  The `keytabs/` directory and contained files are readable only by the Greenplum Database installation user, typically `gpadmin`. |
| lib/     | The location of [user-added runtime dependencies](reg_jar_depend.html). The `native/` subdirectory is the default PXF runtime directory for native libraries. |
| logs/                | The PXF runtime [log file](cfg_logging.html) directory. The `logs/` directory and log files are readable only by the Greenplum Database installation user, typically `gpadmin`. |
| run/         | The default PXF run directory. After starting PXF, this directory contains a PXF process id file, `pxf-app.pid`. `run/` and contained files and directories are readable only by the Greenplum Database installation user, typically `gpadmin`. |
| servers/  | The configuration directory for [PXF servers](cfg_server.html); each subdirectory contains a server definition, and the name of the subdirectory identifies the name of the server. The default server is named `default`. The Greenplum Database administrator may configure other servers. |

Refer to [Configuring PXF](instcfg_pxf.html) and [Starting PXF](cfginitstart_pxf.html#start_pxf) for detailed information about the PXF configuration and startup commands and procedures.

## <a id="movebase"></a>Relocating $PXF_BASE

If you require that `$PXF_BASE` reside in a directory distinct from `<PXF_INSTALL_DIR>`, you can change it from the default location to a location of your choosing after you install PXF 6.x.

PXF provides the [pxf [cluster] prepare](ref/pxf-cluster.html) command to prepare a new `$PXF_BASE` location. The command copies the runtime and configuration directories identified above to the file system location that you specify in a `PXF_BASE` environment variable.

For example, to relocate `$PXF_BASE` to the `/path/to/dir` directory on all Greenplum hosts, run the command as follows:

``` shell
gpadmin@coordinator$ PXF_BASE=/path/to/dir pxf cluster prepare
```

When your `$PXF_BASE` is different than `<PXF_INSTALL_DIR>`, inform PXF by setting the `PXF_BASE` environment variable when you run a `pxf` command:

``` pre
gpadmin@coordinator$ PXF_BASE=/path/to/dir pxf cluster start
```

Set the environment variable in the `.bashrc` shell initialization script for the PXF installation owner (typically the `gpadmin` user) as follows:

``` pre
export PXF_BASE=/path/to/dir
```

