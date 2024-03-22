#!/usr/bin/env bash

set -eo pipefail

if [ "${BASH_VERSION::1}" -lt 4 ]; then
  echo "This script uses the mapfile command which was added in bash version 4.0.0"
  echo "The bash picked by /usr/bin/env is version $BASH_VERSION"
  echo "Please upgrade the bash version to a version >= 4.0.0"
  echo "And add the path to the directory where it is installed to your PATH variable before /bin entry"
  exit 1
fi

WORKING_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd )

_die() {
	local rc=$1
	shift
	echo "$*"
	exit "$rc"
}

_process_scripts() {
	local script{,s} script_path
	mapfile -t scripts < <(grep -o '{{[[:space:]]*SCRIPT[[:space:]][^[:space:]]*[[:space:]]*}}' "sql/${testname}.sql" | awk '{print $3}' | sort | uniq)
	(( ${#scripts[@]} == 0 )) && return
	for script in "${scripts[@]}"; do
		script_path=$(find scripts -iname "${script}")
		[[ -z ${script_path} ]] && _die 2 "Couldn't find script file '${script}'"
		SED_ARGS+=(-e "s|{{[[:space:]]*SCRIPT[[:space:]]*${script//./\\.}[[:space:]]*}}|${WORKING_DIR}/scripts/generated/_${FULL_TESTNAME}_${script}|g")
		sed "${SED_ARGS[@]}" "${script_path}" >"scripts/generated/_${FULL_TESTNAME}_${script}"
	done
}

_host_is_local() {
	local hosts host_regex hostname=${1}
	[[ -z ${hostname} ]] && return 0 # empty host, then we are local
	read -ra hosts <<< "$(grep < /etc/hosts -v '^#.*' | grep -E '(127.0.0.1|::1)' | tr '\n' ' ')"
	host_regex=$({ for h in "${hosts[@]}"; do echo "$h"; done } | sort | uniq | tr '\n' '|')
	host_regex="(${host_regex%|})"
	[[ $hostname =~ $host_regex ]]
}

_gen_uuid() {
	local uuid
	if [[ $(uname) = Darwin ]]; then
		uuid=$(uuidgen)
	else
		uuid=$(cat /proc/sys/kernel/random/uuid)
	fi
	echo "$uuid" | tr '[:upper:]' '[:lower:]' | tr '-' '_'
}

case ${HCFS_PROTOCOL} in
	abfss|ABFSS)
		HCFS_SCHEME=abfss://
		SERVER_CONFIG=${SERVER_CONFIG:-abfss}
		HCFS_PROTOCOL=abfss
		;;
	gs|gcs|GS|GCS)
		HCFS_SCHEME=gs://
		SERVER_CONFIG=${SERVER_CONFIG:-gs}
		HCFS_PROTOCOL=gs
		;;
	minio|MINIO)
		HCFS_SCHEME=s3a://
		SERVER_CONFIG=${SERVER_CONFIG:-minio}
		HCFS_PROTOCOL=s3 # there's no minio protocol
		;;
	s3a|S3A|s3|S3)
		HCFS_SCHEME=s3a://
		SERVER_CONFIG=${SERVER_CONFIG:-s3}
		HCFS_PROTOCOL=s3
		;;
	wasbs|WASBS)
		HCFS_SCHEME=wasbs://
		SERVER_CONFIG=${SERVER_CONFIG:-wasbs}
		HCFS_PROTOCOL=wasbs
		;;
	*) # regular HDFS
		HCFS_PROTOCOL=hdfs
		SERVER_CONFIG=${SERVER_CONFIG:-default}
		unset HCFS_SERVER HCFS_BUCKET HCFS_SCHEME
		;;
esac

# server string for DDL, only needed when not default
if [[ ${SERVER_CONFIG} != default ]]; then
	SERVER_PARAM="\&SERVER=${SERVER_CONFIG}"
fi

HIVE_HOST=${HIVE_HOST:-localhost}
HIVE_PRINCIPAL=${HIVE_PRINCIPAL:+";principal=${HIVE_PRINCIPAL}"}

SED_ARGS=(
	-e "s|{{[[:space:]]*PGHOST[[:space:]]*}}|${PGHOST}|g"
	-e "s|{{[[:space:]]*HCFS_BUCKET[[:space:]]*}}|${HCFS_BUCKET}|g"
	-e "s|{{[[:space:]]*HCFS_CMD[[:space:]]*}}|${HCFS_CMD}|g"
	-e "s|{{[[:space:]]*SERVER_CONFIG[[:space:]]*}}|${SERVER_CONFIG}|g"
	-e "s|{{[[:space:]]*HCFS_PROTOCOL[[:space:]]*}}|${HCFS_PROTOCOL}|g"
	-e "s|{{[[:space:]]*HCFS_SCHEME[[:space:]]*}}|${HCFS_SCHEME}|g"
	-e "s|{{[[:space:]]*SERVER_PARAM[[:space:]]*}}|${SERVER_PARAM}|g"
	-e "s|{{[[:space:]]*PXF_BASE[[:space:]]*}}|${PXF_BASE}|g"
	-e "s|{{[[:space:]]*HBASE_CMD[[:space:]]*}}|${HBASE_CMD}|g"
	-e "s|{{[[:space:]]*BEELINE_CMD[[:space:]]*}}|${BEELINE_CMD}|g"
	-e "s|{{[[:space:]]*HIVE_HOST[[:space:]]*}}|${HIVE_HOST}|g"
	-e "s|{{[[:space:]]*HIVE_PRINCIPAL[[:space:]]*}}|${HIVE_PRINCIPAL}|g"
)

# delete the cleanup steps if we have debug on
if [[ $1 == --debug ]]; then
	SED_ARGS+=(-e '/{{[[:space:]]*CLEAN_UP[[:space:]]*}}/d')
	shift
else
	SED_ARGS+=(-e 's|{{[[:space:]]*CLEAN_UP[[:space:]]*}}||g')
fi

if _host_is_local "${HIVE_HOST}"; then
	# if hive is local delete whole line
	SED_ARGS+=(-e '/{{[[:space:]]*HIVE_REMOTE[[:space:]]*}}/d')
else
	# otherwise delete the flag
	SED_ARGS+=(-e 's|{{[[:space:]]*HIVE_REMOTE[[:space:]]*}}||g')
fi

if _host_is_local "${PGHOST}"; then
	# if GPDB is local delete whole line
	SED_ARGS+=(-e '/{{[[:space:]]*GPDB_REMOTE[[:space:]]*}}/d')
else
	# otherwise delete the flag
	SED_ARGS+=(-e 's|{{[[:space:]]*GPDB_REMOTE[[:space:]]*}}||g')
fi

tests=( "$@" )
if [[ $1 =~ ^schedules/.*_schedule ]]; then
	# get list of tests from schedule file, there may be many lines, many on each line
	read -ra tests <<< "$(grep '^test:' "$1" | sed -e 's/^test: *//' -e 's/#.*$//' | tr '\n' ' ')"
fi

num_args=${#SED_ARGS[@]}
for testname in "${tests[@]#_}"; do # remove leading underscore from list
	date=$(date '+%Y_%m_%d_%H_%M_%S')
	uuid=$(_gen_uuid)
	FULL_TESTNAME=${testname}_${date}_${uuid}
	TEST_LOCATION=/tmp/pxf_automation_data/${testname}/${date}_${uuid}
	SED_ARGS+=(
		-e "s|{{[[:space:]]*TEST_LOCATION[[:space:]]*}}|${TEST_LOCATION}|g"
		-e "s|{{[[:space:]]*FULL_TESTNAME[[:space:]]*}}|${FULL_TESTNAME}|g"
	)
	if [[ -n ${GPDB_5X_STABLE} ]]; then
		SED_ARGS+=(
			-e "s|{{[[:space:]]*5X_CREATE_EXTENSION[[:space:]]*}}|CREATE EXTENSION PXF;|g"
			-e "s|{{[[:space:]]*POSTGRES_COPY_CSV[[:space:]]*}}|CSV|g"
		)
	else
		SED_ARGS+=(
			-e "/{{[[:space:]]*5X_CREATE_EXTENSION[[:space:]]*}}/d"
			-e "s|{{[[:space:]]*POSTGRES_COPY_CSV[[:space:]]*}}|(FORMAT 'csv')|g"
		)
	fi
	_process_scripts
	sed "${SED_ARGS[@]}" "sql/${testname}.sql" >"sql/_${testname}.sql"
	sed "${SED_ARGS[@]}" "expected/${testname}.out" >"expected/_${testname}.out"
	# generate HCFS cleanup script for make clean target
	if grep '{{[[:space:]]*HCFS_CMD[[:space:]]*}}' "sql/${testname}.sql" >/dev/null; then
		: "${HCFS_CMD:?HCFS_CMD must be set}"
		sed "${SED_ARGS[@]}" scripts/cleanup_hcfs.bash >"scripts/_${FULL_TESTNAME}_cleanup_hcfs.bash"
		chmod +x "scripts/_${FULL_TESTNAME}_cleanup_hcfs.bash"
	fi
	# remove the last two args we added, they should not be re-used
	SED_ARGS=( "${SED_ARGS[@]:0:${num_args}}" )
done
