# Disable repacking of jars, since it takes forever
%define __jar_repack %{nil}

# Disable automatic dependency processing both for requirements and provides
AutoReqProv: no

Name: pxf-gp6
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
inside HDFS files, Hive tables, HBase tables and more.

%prep
# If the pxf_version macro is not defined, it gets interpreted as a literal string, need %% to escape it
if [ %{pxf_version} = '%%{pxf_version}' ] ; then
  echo "The macro (variable) pxf_version must be supplied as rpmbuild ... --define='pxf_version [VERSION]'"
  exit 1
fi

%install
%__mkdir -p %{buildroot}/%{prefix}
%__cp -R %{_sourcedir}/* %{buildroot}/%{prefix}

%files
%{prefix}

%preun
# cleanup files and directories created by 'pxf init' command
%__rm -rf %{prefix}/run
