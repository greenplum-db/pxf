#!/usr/bin/env bash

set -x
#fetches official HDP tarball

server=$1
tarball=$2
final_archive=$2
version=$3
platform=$4
distro=$5
major_version=$(echo ${version}| cut -c1)
url=${server}/${distro}/${platform}/${major_version}.x/updates/${version}/${tarball}
destination_dir=${tarball}-data

echo Latest artifact: ${tarball} | tee -a ${log_file}
echo Downloading: ${url} | tee -a ${log_file}

wget ${url}

if [[ $? -ne 0 ]]; then
	echo download failed
	exit 1
fi

echo Untarring artifact
tar xvzf ${tarball} --strip-components 2
rm -rf tars/source
mv ${tarball} "${tarball}.bak"
# rm $tarball
touch ${final_archive}
mkdir -p ${destination_dir}
mv tars/* ${destination_dir}/
rm -rf tars
pushd ${destination_dir}
find . -iwholename "*source.tar.gz" | xargs rm

# Remove tars that are in the root directory
rm *tar.gz

for file in `find . -iwholename "*${version}*tar.gz" | grep "\(tez\|hadoop\|hbase\|zookeeper\|hive\)" | grep -v -E "phoenix|accumulo|storm|calcite_hive3|tez_hive2|sqoop|plugin|lzo" | grep -v -E "tez-[0-9.-]*-minimal"`; do
	mv ${file} .
done;
rm -r -- */
tar czf ${final_archive} *
mv ${final_archive} ../
popd
rm -rf ${destination_dir}
