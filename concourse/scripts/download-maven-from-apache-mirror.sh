#!/usr/bin/env bash

set -e
set -u

PXF_HOME=${HOME}/workspace/pxf/downloads/

MAVEN_VERSION="${1:?a Maven version must be provided}"

maven_dist="apache-maven-${MAVEN_VERSION}-bin.tar.gz"
maven_full_path="maven/maven-3/${MAVEN_VERSION}/binaries/${maven_dist}"

response_json="$(curl -fsSL "https://www.apache.org/dyn/closer.lua/${maven_full_path}?as_json")"
in_dist="$(jq -r '.in_dist // false' <<<"$response_json")"

if [ "$in_dist" == "false" ]; then
    echo >&2 "${maven_dist} was not found in dist; attempting to download from archive.apache.org"
    download_url="https://archive.apache.org/dist/${maven_full_path}"
else
    preferred="$(jq -r ".preferred" <<<"$response_json")"
    path_info="$(jq -r ".path_info" <<<"$response_json")"
    if [ -z "$preferred" ] || [ -z "$path_info" ]; then
        echo >&2 "unable to get download URL from response"
        echo >&2 "$response_json"
        exit 1
    fi
    download_url="${preferred%/}/$path_info"
fi

echo "Downloading $download_url to ${PXF_HOME}/${maven_dist}..."
curl -Lo ${PXF_HOME}/${maven_dist} "$download_url"
