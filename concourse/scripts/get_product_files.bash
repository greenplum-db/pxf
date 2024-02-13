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
		jq --raw-output --argjson gpdb "\"${GPDB_VERSION}\"" --argjson m "${VERSIONS_BEFORE_LATEST}" \
		'sort_by(.version | split(".") | select(.[0] == $gpdb) | map(tonumber))[-1-$m].version'
)
echo -e "Latest - ${VERSIONS_BEFORE_LATEST} GPDB version found:\n${GPDB_VERSION}X:\t${gpdb_version}"

# get the string of product names and turn it into list with correct gpdb_version found above
IFS=, read -ra product_files_GPDB_VERSION <<<"${LIST_OF_PRODUCTS}"
for file in "${product_files_GPDB_VERSION[@]}"; do
	: "product_files/Pivotal-Greenplum/${file/GPDB_VERSION/${gpdb_version}}" # replace with version we are set to grab
	product_files+=("${_}")
done

IFS=, read -ra product_dirs <<<"${LIST_OF_DIRS}"

# for this gpdb version, get the full list of product files
product_files_json=$(pivnet --format=json product-files "--product-slug=${PRODUCT_SLUG}" --release-version "${gpdb_version}")
for ((i = 0; i < ${#product_files[@]}; i++)); do
	file=${product_files[$i]}
	# find the version of GPDB already in the bucket
	download_path=${product_dirs[$i]}/${file##*/}
	# does the version in the bucket match what the list we pulled from pivnet?
	[[ -e ${download_path} ]] || {
	  # if it doesn't exist, look in the previous bucket for that version
		echo "${download_path} does not exist, looking for version Latest - $((VERSIONS_BEFORE_LATEST - 1))"
		new_version=$((VERSIONS_BEFORE_LATEST - 1))
		# this assumes we are naming our paths with latest-X !
		download_path=${download_path/latest-${VERSIONS_BEFORE_LATEST}\//latest-${new_version}/}
	}
	# if the version of gpdb exists in the i-1 bucket but not the i-th bucket, check the SHA
	if [[ -e ${download_path} ]]; then
		echo "Found file ${download_path}, checking sha256sum..."
		sha256=$(jq <<<"${product_files_json}" -r --arg object_key "${file}" '.[] | select(.aws_object_key == $object_key).sha256')
		sum=$(sha256sum "${download_path}" | cut -d' ' -f1)
		if [[ ${sum} == "${sha256}" ]]; then
			echo "Sum is equivalent, skipping download of ${file}..."
			# if the SHAs match, bring the version of gpdb in the i-1 bucket into the i-th bucket
			if [[ ! ${download_path} =~ ^${product_dirs[$i]} ]]; then
				echo "Cleaning ${product_dirs[$i]} and copying file ${download_path} to ${product_dirs[$i]}..."
				rm -f "${product_dirs[$i]}"/*.{rpm,deb}
				cp "${download_path}" "${product_dirs[$i]}"
			fi
			continue
		fi
	fi
	# if the version doesn't exist in either i-1, i-th bucket or the SHAs don't match, download it from pivnet.
	id=$(jq <<<"${product_files_json}" -r --arg object_key "${file}" '.[] | select(.aws_object_key == $object_key).id')

	if [[ -z "${id}" ]]; then
		echo "Did not find '${file}' in product files for GPDB '${gpdb_version}'"

		case "${file}" in
			*rhel7*) existing_file="$(find ${product_dirs[$i]}/ -name *rhel7*.rpm)" ;;
			*rhel8*) existing_file="$(find ${product_dirs[$i]}/ -name *rhel8*.rpm)" ;;
			*rhel9*) existing_file="$(find ${product_dirs[$i]}/ -name *rhel9*.rpm)" ;;
			*ubuntu18*) existing_file="$(find ${product_dirs[$i]}/ -name *ubuntu18*.deb)" ;;
			*)
				echo "Unexpected file: ${file}"
				exit 1;;
		esac

		echo "Keeping existing file: ${existing_file}"
		continue
	fi
	echo "Cleaning ${product_dirs[$i]} and downloading ${file} with id ${id} to ${product_dirs[$i]}..."
	rm -f "${product_dirs[$i]}"/*.{rpm,deb}
	pivnet download-product-files \
		"--download-dir=${product_dirs[$i]}" \
		"--product-slug=${PRODUCT_SLUG}" \
		"--release-version=${gpdb_version}" \
		"--product-file-id=${id}" >/dev/null 2>&1 &
	pids+=($!)
done

wait "${pids[@]}"
