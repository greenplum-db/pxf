#!/usr/bin/env bash

set -eo pipefail

export PGHOST=cdw
export PGUSER=gpadmin
export PGDATABASE=tpch
GPHOME="/usr/local/greenplum-db-devel"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HADOOP_HOSTNAME="ccp-$(cat terraform_dataproc/name)-m"
SCALE=${SCALE:-10}
scale=$(($SCALE + 0))
# whether PXF is being installed from a new component-based packaging
PXF_COMPONENT=${PXF_COMPONENT:=false}
if [[ ${PXF_COMPONENT} == "true" ]]; then
    PXF_HOME=/usr/local/pxf-gp${GP_VER}
else
    PXF_HOME=${GPHOME}/pxf
fi
PXF_SERVER_DIR="${PXF_HOME}/servers"
UUID=$(cat /proc/sys/kernel/random/uuid)

if [[ ${scale} -gt 10 ]]; then
    VALIDATION_QUERY="SUM(l_partkey) AS PARTKEYSUM"
else
    VALIDATION_QUERY="COUNT(*) AS Total, COUNT(DISTINCT l_orderkey) AS ORDERKEYS, SUM(l_partkey) AS PARTKEYSUM, COUNT(DISTINCT l_suppkey) AS SUPPKEYS, SUM(l_linenumber) AS LINENUMBERSUM"
fi

LINEITEM_COUNT="unset"
LINEITEM_VAL_RESULTS="unset"
source "${CWDIR}/pxf_common.bash"

###########################################
## TABLE CREATION FUNCTIONS
###########################################

function readable_external_table_text_query() {
    local name=${1}
    local path=${2}
    local delimiter=${3:-'|'}
    psql -c "CREATE READABLE EXTERNAL TABLE lineitem_${name}_read (LIKE lineitem) LOCATION('${path}') FORMAT 'CSV' (DELIMITER '${delimiter}')"
}

function writable_external_table_text_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_${name}_write (LIKE lineitem) LOCATION('${path}') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function readable_external_table_parquet_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE READABLE EXTERNAL TABLE lineitem_${name}_read_parquet (LIKE lineitem) LOCATION('${path}') FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8'"
}

function writable_external_table_parquet_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_${name}_write_parquet (LIKE lineitem) LOCATION('${path}') FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export') DISTRIBUTED BY (l_partkey)"
}

###########################################
## UTILITY FUNCTIONS
###########################################

function setup_sshd() {
    # kill the sshd background process when this script exits. Otherwise, the
    # concourse build will run forever.
    trap 'pkill sshd' EXIT
    /usr/sbin/sshd &
    passwd -u root

    if [[ -d cluster_env_files ]]; then
        /bin/cp -Rf cluster_env_files/.ssh/* /root/.ssh
        /bin/cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
        /bin/cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
        /bin/cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    fi
}

function write_header() {
    echo -ne "\n\n############################################\n# ${1}\n############################################\n"
}

function write_sub_header() {
    echo -ne "\n${1}\n------------------------------\n"
}

function read_and_validate_table_count() {
    local table_name="$1"
    local expected_count="$2"
    local num_rows=$(time psql -t -c "SELECT COUNT(l_linenumber) FROM $table_name" | tr -d ' ')

    if [[ ${num_rows} != ${expected_count} ]]; then
        echo "Expected number of rows in table ${table_name} to be ${expected_count} but was ${num_rows}"
        exit 1
    fi
}

function sync_configuration() {
    gpssh -u gpadmin -h cdw -v -s -e "${PXF_HOME}/bin/pxf cluster sync"
}

function create_database_and_schema() {
    # Create DB
    psql -d postgres <<EOF
DROP DATABASE IF EXISTS tpch;
CREATE DATABASE tpch;
\c tpch;
CREATE TABLE lineitem (
    l_orderkey    BIGINT NOT NULL,
    l_partkey     BIGINT NOT NULL,
    l_suppkey     BIGINT NOT NULL,
    l_linenumber  BIGINT NOT NULL,
    l_quantity    DECIMAL(15,2) NOT NULL,
    l_extendedprice  DECIMAL(15,2) NOT NULL,
    l_discount    DECIMAL(15,2) NOT NULL,
    l_tax         DECIMAL(15,2) NOT NULL,
    l_returnflag  CHAR(1) NOT NULL,
    l_linestatus  CHAR(1) NOT NULL,
    l_shipdate    DATE NOT NULL,
    l_commitdate  DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode     CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
) DISTRIBUTED BY (l_partkey);
EOF
}

function initial_data_load() {
    psql -c "CREATE EXTERNAL TABLE lineitem_external (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    echo -ne "\nInitial data load from external into GPDB..."
    LINEITEM_COUNT=$(time psql -c "INSERT INTO lineitem SELECT * FROM lineitem_external" | awk '{print $3}')
#    echo -ne "\nValidating initial data load..."
#    validate_write_to_gpdb "lineitem_external" "lineitem"
    echo -ne "\nInitial data load and validation complete\n"
    echo -ne "${LINEITEM_COUNT} items loaded into the GPDB"
}

function validate_write_to_gpdb() {
    local external=${1}
    local internal=${2}
    local external_values
    local gpdb_values

    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${internal}")
    write_sub_header "Results from GPDB query"
    echo ${gpdb_values}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${external}")
    write_sub_header "Results from external query"
    echo ${external_values}

    if [[ "${external_values}" != "${gpdb_values}" ]]; then
        echo ERROR! Unable to validate data written from external to GPDB
        exit 1
    fi
}

function validate_write_to_external() {
    local benchmark_name=${1}
    local path=${2}
    local name="${benchmark_name}_from_write"

    readable_external_table_text_query "${name}" "${path}" ","

    if [[ ${LINEITEM_VAL_RESULTS} == unset ]]; then
        LINEITEM_VAL_RESULTS=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem")
    fi

    write_sub_header "Results from GPDB query"
    echo ${LINEITEM_VAL_RESULTS}

    local external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem_${name}_read")
    write_sub_header "Results from external query"
    echo ${external_values}

    if [[ "${external_values}" != "${LINEITEM_VAL_RESULTS}" ]]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

###########################################
## TEST CONFIGURATION SECTION
###########################################

function configure_adl_server() {
    ADL_SERVER_DIR="${PXF_SERVER_DIR}/adlbenchmark"
    # Create the ADL Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h cdw -v -s -e "mkdir -p $ADL_SERVER_DIR && cp ${PXF_HOME}/templates/adl-site.xml $ADL_SERVER_DIR"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_ADL_REFRESH_URL|${ADL_REFRESH_URL}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_ADL_CLIENT_ID|${ADL_CLIENT_ID}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_ADL_CREDENTIAL|${ADL_CREDENTIAL}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    sync_configuration
}

function create_adl_tables() {
    local name=${1}
    readable_external_table_text_query "${name}" "pxf://${ADL_ACCOUNT}.azuredatalakestore.net/adl-profile-test/lineitem/${SCALE}/?PROFILE=adl:text&server=adlbenchmark"
    writable_external_table_text_query "${name}" "pxf://${ADL_ACCOUNT}.azuredatalakestore.net/adl-profile-test/output/${SCALE}/${UUID}/?PROFILE=adl:text&server=adlbenchmark"
}

function configure_gcs_server() {
    cat << EOF > /tmp/gsc-ci-service-account.key.json
${GOOGLE_CREDENTIALS}
EOF

    GS_SERVER_DIR="${PXF_SERVER_DIR}/gsbenchmark"
    gpssh -u gpadmin -h cdw -v -s -e "mkdir -p $GS_SERVER_DIR && cp ${PXF_HOME}/templates/gs-site.xml $GS_SERVER_DIR"
    gpscp -u gpadmin -h cdw /tmp/gsc-ci-service-account.key.json =:${GS_SERVER_DIR}/
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_GOOGLE_STORAGE_KEYFILE|${GS_SERVER_DIR}/gsc-ci-service-account.key.json|\" ${GS_SERVER_DIR}/gs-site.xml"
    sync_configuration
}

function create_gcs_text_tables() {
    local name=${1}
    local run_id=${2}
    # create text tables
    readable_external_table_text_query "${name}" "pxf://data-gpdb-ud-tpch/${SCALE}/lineitem_data/?PROFILE=gs:text&SERVER=gsbenchmark"
    writable_external_table_text_query "${name}" "pxf://data-gpdb-ud-pxf-benchmark/output/${SCALE}/${UUID}-${run_id}/?PROFILE=gs:text&SERVER=gsbenchmark"
}

function create_gcs_parquet_tables() {
    local name=${1}
    local run_id=${2}
    # create parquet tables
    readable_external_table_parquet_query "${name}" "pxf://data-gpdb-ud-pxf-benchmark/gs-profile-parquet-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=gs:parquet&SERVER=gsbenchmark"
    writable_external_table_parquet_query "${name}" "pxf://data-gpdb-ud-pxf-benchmark/gs-profile-parquet-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=gs:parquet&SERVER=gsbenchmark"
}

function create_gphdfs_tables() {
    local name=${1}
    readable_external_table_text_query "${name}" "gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_read/"
    writable_external_table_text_query "${name}" "gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_gphdfs_write/"
}

function create_hadoop_text_tables() {
    local name=${1}
    local run_id=${2}
    # create text tables
    readable_external_table_text_query "${name}" "pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple"
    writable_external_table_text_query "${name}" "pxf://tmp/lineitem_hadoop_write/${run_id}/?PROFILE=HdfsTextSimple"
}

function create_hadoop_parquet_tables() {
    local name=${1}
    local run_id=${2}
    # create parquet tables
    readable_external_table_parquet_query "${name}" "pxf://tmp/lineitem_write_parquet/${run_id}/?PROFILE=hdfs:parquet"
    writable_external_table_parquet_query "${name}" "pxf://tmp/lineitem_write_parquet/${run_id}/?PROFILE=hdfs:parquet"
}

function configure_s3_extension() {
    psql -c "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '\$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE"
    psql -c "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '\$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE"
    psql -c "CREATE PROTOCOL s3 (writefunc = write_to_s3, readfunc = read_from_s3)"

    cat > /tmp/s3.conf <<EOF
[default]
accessid = "${AWS_ACCESS_KEY_ID}"
secret = "${AWS_SECRET_ACCESS_KEY}"
threadnum = 4
chunksize = 67108864
low_speed_limit = 10240
low_speed_time = 60
encryption = true
version = 1
proxy = ""
autocompress = false
verifycert = true
server_side_encryption = ""
# gpcheckcloud config
gpcheckcloud_newline = "\n"
EOF
    cat cluster_env_files/etc_hostfile | grep sdw | cut -d ' ' -f 2 > /tmp/segment_hosts
    gpssh -u gpadmin -f /tmp/segment_hosts -v -s -e 'mkdir ~/s3/'
    gpscp -u gpadmin -f /tmp/segment_hosts /tmp/s3.conf =:~/s3/s3.conf
}

function create_s3_extension_tables() {
    local name=${1}
    local run_id=${2}
    readable_external_table_text_query "${name}" "s3://s3.us-east-1.amazonaws.com/gpdb-ud-scratch/s3-profile-test/lineitem/${SCALE}/ config=/home/gpadmin/s3/s3.conf"
    writable_external_table_text_query "${name}" "s3://s3.us-east-2.amazonaws.com/gpdb-ud-pxf-benchmark/s3-profile-test/output/${SCALE}/${UUID}-${run_id}/ config=/home/gpadmin/s3/s3.conf"
}

function configure_s3_server() {
    # We need to create s3-site.xml and provide AWS credentials
    S3_SERVER_DIR="${PXF_SERVER_DIR}/s3benchmark"
    # Special configuration for reading parquet files
    S3_SERVER_DIR_PARQUET="${PXF_SERVER_DIR}/s3benchmarkparquet"
    # Make a backup of core-site and update it with the S3 core-site
    gpssh -u gpadmin -h cdw -v -s -e "mkdir -p $S3_SERVER_DIR && cp ${PXF_HOME}/templates/s3-site.xml $S3_SERVER_DIR"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_AWS_ACCESS_KEY_ID|${AWS_ACCESS_KEY_ID}|\" $S3_SERVER_DIR/s3-site.xml"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_AWS_SECRET_ACCESS_KEY|${AWS_SECRET_ACCESS_KEY}|\" $S3_SERVER_DIR/s3-site.xml"

    gpssh -u gpadmin -h cdw -v -s -e "cp -R $S3_SERVER_DIR $S3_SERVER_DIR_PARQUET"
    # Improves reading from parquet files for S3
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|</configuration>|<property><name>fs.s3a.experimental.input.fadvise</name><value>random</value></property></configuration>|\" $S3_SERVER_DIR_PARQUET/s3-site.xml"
    sync_configuration
}

function create_s3_text_tables() {
    local name=${1}
    local run_id=${2}
    # create text tables
    readable_external_table_text_query "${name}" "pxf://gpdb-ud-scratch/s3-profile-test/lineitem/${SCALE}/?PROFILE=s3:text&SERVER=s3benchmark"
    writable_external_table_text_query "${name}" "pxf://gpdb-ud-pxf-benchmark/s3-profile-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=s3:text&SERVER=s3benchmark"
}

function create_s3_parquet_tables() {
    local name=${1}
    local run_id=${2}
    # create parquet tables
    readable_external_table_parquet_query "${name}" "pxf://gpdb-ud-pxf-benchmark/s3-profile-parquet-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=s3:parquet&SERVER=s3benchmarkparquet"
    writable_external_table_parquet_query "${name}" "pxf://gpdb-ud-pxf-benchmark/s3-profile-parquet-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=s3:parquet&SERVER=s3benchmarkparquet"
}

function configure_wasb_server() {
    WASB_SERVER_DIR="${PXF_SERVER_DIR}/wasbbenchmark"
    # Create the WASB Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h cdw -v -s -e "mkdir -p $WASB_SERVER_DIR && cp ${PXF_HOME}/templates/wasbs-site.xml $WASB_SERVER_DIR"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_NAME|${WASB_ACCOUNT_NAME}|\" ${WASB_SERVER_DIR}/wasbs-site.xml"
    gpssh -u gpadmin -h cdw -v -s -e "sed -i \"s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_KEY|${WASB_ACCOUNT_KEY}|\" ${WASB_SERVER_DIR}/wasbs-site.xml"
    sync_configuration
}

function create_wasb_tables() {
    local name=${1}
    readable_external_table_text_query ${name} "pxf://pxf-container@${WASB_ACCOUNT_NAME}.blob.core.windows.net/wasb-profile-test/lineitem/${SCALE}/?PROFILE=wasbs:text&server=wasbbenchmark"
    writable_external_table_text_query ${name} "pxf://pxf-container@${WASB_ACCOUNT_NAME}.blob.core.windows.net/wasb-profile-test/output/${SCALE}/${UUID}/?PROFILE=wasbs:text&server=wasbbenchmark"
}

###########################################
## BENCHMARK FUNCTIONS
###########################################

function run_concurrent_benchmark() {
    local benchmark_fn=${1}
    local prepare_test_fn=${2}
    local benchmark_name=${3}
    local benchmark_description=${4}
    local concurrency=${5}
    local pids=()
    local status_codes=()
    local has_failures=0

    for i in `seq 1 ${concurrency}`; do
        echo "Starting PXF Benchmark ${benchmark_fn} ${i} with UUID ${UUID}-${i}"
        ${benchmark_fn} ${prepare_test_fn} "${benchmark_name}" "${benchmark_description}" "${i}" >/tmp/${benchmark_fn}-${benchmark_name}-${i}.bench 2>&1 &
        pids+=("$!")
        # Sleep for 30 seconds before subsequent runs
        sleep 30
    done

    set +e
    # collect status codes from background tasks
    for p in "${pids[@]}"; do
        wait ${p}
        status_code=$?
        status_codes+=("${status_code}")
    done
    set -e

    # print out all the results from the files
    cat $(ls /tmp/${benchmark_fn}-${benchmark_name}-*.bench)

    # check for errors in background tasks
    local has_failures=0
    for i in `seq 1 ${concurrency}`; do
        if [[ ${status_codes[i-1]} != 0 ]]; then
            echo "Run ${i} failed"
            has_failures=1
        fi
    done

    if [[ ${has_failures} != 0 ]]; then
        exit 1
    fi
}

function run_text_benchmark() {
    local prepare_test_fn=${1}
    local benchmark_name=${2}
    local benchmark_description=${3}
    local run_id=${4}
    local name="${benchmark_name}_${run_id}"

    ${prepare_test_fn} "${name}" "${run_id}"

    write_header "${benchmark_description} READ TEXT BENCHMARK (Run ${run_id})"
    read_and_validate_table_count "lineitem_${name}_read" "${LINEITEM_COUNT}"

    write_header "${benchmark_description} WRITE TEXT BENCHMARK (Run ${run_id})"
    local write_count=$(time psql -c "INSERT INTO lineitem_${name}_write SELECT * FROM lineitem" | awk '{print $3}')

    if [[ "${write_count}" != "${LINEITEM_COUNT}" ]]; then
        echo "ERROR! Unable to validate text data written from GPDB to external. Expected ${LINEITEM_COUNT}, got ${write_count}"
        exit 1
    fi
}

function run_parquet_benchmark() {
    local prepare_test_fn=${1}
    local benchmark_name=${2}
    local benchmark_description=${3}
    local run_id=${4}
    local name="${benchmark_name}_${run_id}"

    ${prepare_test_fn} "${name}" "${run_id}"

    write_header "${benchmark_description} WRITE PARQUET BENCHMARK (Run ${run_id})"
    local write_parquet_count=$(time psql -c "INSERT INTO lineitem_${name}_write_parquet SELECT * FROM lineitem" | awk '{print $3}')

    if [[ "${write_parquet_count}" != "${LINEITEM_COUNT}" ]]; then
        echo "ERROR! Unable to validate parquet data written from GPDB to external. Expected ${LINEITEM_COUNT}, got ${write_parquet_count}"
        exit 1
    fi

    write_header "${benchmark_description} READ PARQUET BENCHMARK (Run ${run_id})"
    read_and_validate_table_count "lineitem_${name}_read_parquet" "${LINEITEM_COUNT}"
}

function main() {
    setup_sshd
    remote_access_to_gpdb

    if [[ ${PXF_COMPONENT} == "true" ]]; then
        install_gpdb_package
        setup_gpadmin_user
        install_pxf_tarball
    else
        install_gpdb_binary # Installs the GPDB Binary on the container
        setup_gpadmin_user
        install_pxf_server
    fi

    echo "Running ${SCALE}G test with UUID ${UUID}"
    echo "PXF Process Details:"
    echo "$(ps aux | grep tomcat)"

    source ${GPHOME}/greenplum_path.sh
    create_database_and_schema
    initial_data_load

    concurrency=${BENCHMARK_CONCURRENCY:-1}

    if [[ ${BENCHMARK_ADL} == true ]]; then
        configure_adl_server
        run_text_benchmark create_adl_tables "ADL" "AZURE DATA LAKE"
    fi

    if [[ ${BENCHMARK_WASB} == true ]]; then
        configure_wasb_server
        run_text_benchmark create_wasb_tables "wasb" "AZURE BLOB STORAGE"
    fi

    if [[ ${BENCHMARK_GCS} == true ]]; then
        configure_gcs_server
        if [[ ${concurrency} == 1 ]]; then
            run_text_benchmark create_gcs_text_tables "gcs" "GOOGLE CLOUD STORAGE" "0"
            run_parquet_benchmark create_gcs_parquet_tables "gcs" "GOOGLE CLOUD STORAGE" "0"
        else
            run_concurrent_benchmark run_text_benchmark create_gcs_text_tables "gcs" "GOOGLE CLOUD STORAGE" "${concurrency}"
            run_concurrent_benchmark run_parquet_benchmark create_gcs_parquet_tables "gcs" "GOOGLE CLOUD STORAGE" "${concurrency}"
        fi
    fi

    if [[ ${BENCHMARK_GPHDFS} == true ]]; then
        run_text_benchmark create_gphdfs_tables "gphdfs" "GPHDFS"
        echo -ne "\n>>> Validating GPHDFS data <<<\n"
        validate_write_to_external "gphdfs" "gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_gphdfs_write/"
    fi

    if [[ ${BENCHMARK_S3_EXTENSION} == true ]]; then
        configure_s3_extension
        if [[ ${concurrency} == 1 ]]; then
            run_text_benchmark create_s3_extension_tables "s3_c" "S3_EXTENSION"
        else
            run_concurrent_benchmark run_text_benchmark create_s3_extension_tables "s3_c" "S3_EXTENSION" "${concurrency}"
        fi
    fi

    if [[ ${BENCHMARK_S3} == true ]]; then
        configure_s3_server
        if [[ ${concurrency} == 1 ]]; then
            run_text_benchmark create_s3_text_tables "s3" "S3" "0"
            run_parquet_benchmark create_s3_parquet_tables "s3" "S3" "0"
        else
            run_concurrent_benchmark run_text_benchmark create_s3_text_tables "s3" "S3" "${concurrency}"
            run_concurrent_benchmark run_parquet_benchmark create_s3_parquet_tables "s3" "S3" "${concurrency}"
        fi
    fi

    if [[ ${BENCHMARK_HADOOP} == true ]]; then
        if [[ ${concurrency} == 1 ]]; then
            run_text_benchmark create_hadoop_text_tables "hadoop" "HADOOP" "0"
            run_parquet_benchmark create_hadoop_parquet_tables "hadoop" "HADOOP" "0"
            echo -ne "\n>>> Validating HADOOP data <<<\n"
            validate_write_to_external "hadoop" "pxf://tmp/lineitem_hadoop_write/0/?PROFILE=HdfsTextSimple"
        else
            run_concurrent_benchmark run_text_benchmark create_hadoop_text_tables "hadoop" "HADOOP" ${concurrency}
            run_concurrent_benchmark run_parquet_benchmark create_hadoop_parquet_tables "hadoop" "HADOOP" "${concurrency}"
        fi
    fi

    sleep_time=${SLEEP_BEFORE_DESTROY_IN_SEC:-150}
    echo "Destroying cluster in ${sleep_time} seconds"
    sleep "${sleep_time}"
}

main
