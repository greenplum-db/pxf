#!/usr/bin/env bash

set -e

: "${PIVNET_API_TOKEN:?PIVNET_API_TOKEN is required}"
: "${PIVNET_CLI_DIR:?PIVNET_CLI_DIR is required}"
: "${VERSIONS_BEFORE_LATEST:?VERSIONS_BEFORE_LATEST is required}"
: "${LIST_OF_DIRS:?LIST_OF_DIRS is required}"
: "${GPDB_VERSION:?GPDB_VERSION is required}"
: "${PRODUCT_SLUG:?PRODUCT_SLUG is required}"

PATH=${PIVNET_CLI_DIR}:${PATH}
chmod +x "${PIVNET_CLI_DIR}/pivnet"

# log in to pivnet
pivnet login "--api-token=${PIVNET_API_TOKEN}"

# get version numbers in sorted order
# https://stackoverflow.com/questions/57071166/jq-find-the-max-in-quoted-values/57071319#57071319
gpdb_version=$(
	pivnet --format=json releases "--product-slug=${PRODUCT_SLUG}" | \
		jq --raw-output --argjson gpdb "${GPDB_VERSION}" --argjson m "${VERSIONS_BEFORE_LATEST}" \
		'sort_by(.version | split(".") | map(tonumber) | select(.[0] == $gpdb))[-1-$m].version'
)
echo -e "Latest - ${VERSIONS_BEFORE_LATEST} GPDB version found:\n${GPDB_VERSION}X:\t${gpdb_version}"

IFS=, read -ra product_files_GPDB_VERSION <<<"${LIST_OF_PRODUCTS}"
for file in "${product_files_GPDB_VERSION[@]}"; do
	: "product_files/Pivotal-Greenplum/${file/GPDB_VERSION/${gpdb_version}}" # replace with version we are set to grab
	product_files+=("${_}")
done

IFS=, read -ra product_dirs <<<"${LIST_OF_DIRS}"

product_files_json=$(pivnet --format=json product-files "--product-slug=${PRODUCT_SLUG}" --release-version "${gpdb_version}")
for ((i = 0; i < ${#product_files[@]}; i++)); do
	file=${product_files[$i]}
	download_path=${product_dirs[$i]}/${file##*/}
	[[ -e ${download_path} ]] || {
		echo "${download_path} does not exist, looking for version Latest - $((VERSIONS_BEFORE_LATEST - 1))"
		new_version=$((VERSIONS_BEFORE_LATEST - 1))
		# this assumes we are naming our paths with latest-X !
		download_path=${download_path/latest-${VERSIONS_BEFORE_LATEST}\//latest-${new_version}/}
	}
	if [[ -e ${download_path} ]]; then
		echo "Found file ${download_path}, checking sha256sum..."
		sha256=$(jq <<<"${product_files_json}" -r --arg object_key "${file}" '.[] | select(.aws_object_key == $object_key).sha256')
		sum=$(sha256sum "${download_path}" | cut -d' ' -f1)
		if [[ ${sum} == "${sha256}" ]]; then
			echo "Sum is equivalent, skipping download of ${file}..."
			if [[ ! ${download_path} =~ ^${product_dirs[$i]} ]]; then
				echo "Cleaning ${product_dirs[$i]} and copying file ${download_path} to ${product_dirs[$i]}..."
				rm -f "${product_dirs[$i]}"/*.{rpm,deb}
				cp "${download_path}" "${product_dirs[$i]}"
			fi
			continue
		fi
	fi
	rm -f "${product_dirs[$i]}"/*.{rpm,deb}
	id=$(jq <<<"${product_files_json}" -r --arg object_key "${file}" '.[] | select(.aws_object_key == $object_key).id')
	echo "Downloading ${file} with id ${id} to ${product_dirs[$i]}..."
	pivnet download-product-files \
		"--download-dir=${product_dirs[$i]}" \
		"--product-slug=${PRODUCT_SLUG}" \
		"--release-version=${gpdb_version}" \
		"--product-file-id=${id}" >/dev/null 2>&1 &
	pids+=($!)
done

wait "${pids[@]}"
