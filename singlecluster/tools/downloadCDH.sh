#!/usr/bin/env bash

set -x
#fetches official CDH tarball

server='http://archive.cloudera.com'
tarballs=(
  'hadoop-2.6.0-cdh5.12.2.tar.gz'
  'hbase-1.2.0-cdh5.12.2.tar.gz'
  'hive-1.1.0-cdh5.12.2.tar.gz'
  'parquet-1.5.0-cdh5.12.2.tar.gz'
  'parquet-format-2.1.0-cdh5.12.2.tar.gz'
  'pig-0.12.0-cdh5.12.2.tar.gz'
  'zookeeper-3.4.5-cdh5.12.2.tar.gz'
)
distro='cdh'
version='5.12.2'
major_version=$(echo ${version}| cut -c1)
destination_dir=CDH-${version}

rm -r ${destination_dir}
rm ${destination_dir}.tar.gz
mkdir -p ${destination_dir}

for tarball in ${tarballs[@]}
do
  url=${server}/${distro}${major_version}/${distro}/${major_version}/${tarball}
  echo Latest artifact: ${tarball} | tee -a ${log_file}
  echo Downloading: ${url} | tee -a ${log_file}
  wget ${url}
  if [[ $? -ne 0 ]]; then
	  echo download failed
	  exit 1
  fi
  mv ${tarball} ${destination_dir}
done

tar -czf ${destination_dir}.tar.gz ${destination_dir}
rm -rf ${destination_dir}
