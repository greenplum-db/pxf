# Disable repacking of jars, since it takes forever
%define __jar_repack %{nil}

# Disable automatic dependency processing both for requirements and provides
AutoReqProv: no

Name: pxf-gp7
Version: %{pxf_version}
Release: %{pxf_release}%{?dist}
Summary: Greenplum PXF framework for external data access
License: %{license}
URL: https://github.com/greenplum-db/pxf
Vendor: %{vendor}

Prefix: /usr/local/%{name}

# .so file makes sense only when installing on Greenplum node, so inherit Greenplum's dependencies implicitly
# Java server can be installed on a new node, only bash is needed for management scripts
Requires: bash

%description
PXF is an extensible framework that allows a distributed database like Greenplum to query external data files,
whose metadata is not managed by the database. PXF includes built-in connectors for accessing data that exists
inside HDFS files, Hive tables, HBase tables, databases that support JDBC, data stores (S3, GCS) and more.

%prep
# If the pxf_version macro is not defined, it gets interpreted as a literal string, need %% to escape it
if [ %{pxf_version} = '%%{pxf_version}' ] ; then
  echo "The macro (variable) pxf_version must be supplied as rpmbuild ... --define='pxf_version [VERSION]'"
  exit 1
fi

%install
%__mkdir -p %{buildroot}/%{prefix}
%__cp -R %{_sourcedir}/* %{buildroot}/%{prefix}

%post
sed -i "s|directory =.*|directory = '${RPM_INSTALL_PREFIX}/gpextable/'|g" "${RPM_INSTALL_PREFIX}/gpextable/pxf.control"
sed -i "s|module_pathname =.*|module_pathname = '${RPM_INSTALL_PREFIX}/gpextable/pxf'|g" "${RPM_INSTALL_PREFIX}/gpextable/pxf.control"
sed -i "s|directory =.*|directory = '${RPM_INSTALL_PREFIX}/fdw/'|g" "${RPM_INSTALL_PREFIX}/fdw/pxf_fdw.control"
sed -i "s|module_pathname =.*|module_pathname = '${RPM_INSTALL_PREFIX}/fdw/pxf_fdw'|g" "${RPM_INSTALL_PREFIX}/fdw/pxf_fdw.control"

%files
%{prefix}

%pre
# cleanup files and directories created by 'pxf init' command
# only applies for old installations (pre 6.0.0)
%__rm -f "${RPM_INSTALL_PREFIX}/conf/pxf-private.classpath"
%__rm -rf "${RPM_INSTALL_PREFIX}/pxf-service"

%posttrans
# PXF v5 RPM installation removes the run directory during the %preun step.
# The lack of run directory prevents PXF v6+ from starting up.
# %posttrans of the new package is the only step that runs after the %preun
# of the old package
%{__install} -d -m 700 "${RPM_INSTALL_PREFIX}/run"
