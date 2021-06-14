#!/usr/bin/env bash

set -e

: "${PIVNET_CLI_DIR:?PIVNET_CLI_DIR is required}"

PATH=${PIVNET_CLI_DIR}:${PATH}
chmod +x "${PIVNET_CLI_DIR}/pivnet"

pivnet_cli_repo=pivotal-cf/pivnet-cli
latest_pivnet_cli_tag=$(curl --silent "https://api.github.com/repos/${pivnet_cli_repo}/releases/latest" | jq -r .tag_name)
if [[ ${latest_pivnet_cli_tag#v} == $(pivnet --version) ]]; then
	echo "Already have version ${latest_pivnet_cli_tag} of pivnet-cli, skipping download..."
else
	echo "Downloading version ${latest_pivnet_cli_tag} of pivnet-cli..."
	wget -q "https://github.com/${pivnet_cli_repo}/releases/download/${latest_pivnet_cli_tag}/pivnet-linux-amd64-${latest_pivnet_cli_tag#v}" -O "${PIVNET_CLI_DIR}/pivnet"
fi
