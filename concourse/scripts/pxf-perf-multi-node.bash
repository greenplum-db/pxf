#!/usr/bin/env bash

set -eo pipefail

export PGHOST=mdw
export PGUSER=gpadmin
export PGDATABASE=tpch
GPHOME="/usr/local/greenplum-db-devel"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HADOOP_HOSTNAME="ccp-$(cat terraform_dataproc/name)-m"
VALIDATION_QUERY="COUNT(*) AS Total, COUNT(DISTINCT l_orderkey) AS ORDERKEYS, SUM(l_partkey) AS PARTKEYSUM, COUNT(DISTINCT l_suppkey) AS SUPPKEYS, SUM(l_linenumber) AS LINENUMBERSUM"
LINEITEM_COUNT="unset"
source "${CWDIR}/pxf_common.bash"

function create_database_and_schema {
    psql -d postgres <<-EOF
    DROP DATABASE IF EXISTS tpch;
    CREATE DATABASE tpch;
    \c tpch;
    CREATE TABLE lineitem (
        l_orderkey    INTEGER NOT NULL,
        l_partkey     INTEGER NOT NULL,
        l_suppkey     INTEGER NOT NULL,
        l_linenumber  INTEGER NOT NULL,
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
    psql -c "CREATE EXTERNAL TABLE lineitem_external (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '\$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE"
    psql -c "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '\$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE"
    psql -c "CREATE PROTOCOL s3 (writefunc = write_to_s3, readfunc = read_from_s3)"

    cat > /tmp/s3.conf <<-EOF
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

function create_pxf_external_tables {
    psql -c "CREATE EXTERNAL TABLE pxf_lineitem_read (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE pxf_lineitem_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function create_gphdfs_external_tables {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_read_gphdfs/') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE gphdfs_lineitem_write (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function setup_sshd {
    service sshd start
    passwd -u root

    if [ -d cluster_env_files ]; then
        /bin/cp -Rf cluster_env_files/.ssh/* /root/.ssh
        /bin/cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
        /bin/cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
        /bin/cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    fi
}

function write_data {
    local dest
    local source
    dest=${2}
    source=${1}
    psql -c "INSERT INTO ${dest} SELECT * FROM ${source}"
}

function validate_write_to_gpdb {
    local external
    local internal
    local external_values
    local gpdb_values
    external=${1}
    internal=${2}
    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${external}")
    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${internal}")

    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    if [ "${external_values}" != "${gpdb_values}" ]; then
        echo ERROR! Unable to validate data written from external to GPDB
        exit 1
    fi
}

function gphdfs_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read_after_write (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV'"
    local external_values
    local gpdb_values
    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM gphdfs_lineitem_read_after_write")
    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem")

    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    if [ "${external_values}" != "${gpdb_values}" ]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function pxf_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE pxf_lineitem_read_after_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
    local external_values
    local gpdb_values
    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM pxf_lineitem_read_after_write")
    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem")

    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    if [ "${external_values}" != "${gpdb_values}" ]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function run_pxf_benchmark {
    create_pxf_external_tables

    cat << EOF


############################
#    PXF READ BENCHMARK    #
############################
EOF
    time psql -c "SELECT COUNT(*) FROM pxf_lineitem_read"

    cat << EOF


############################
#   PXF WRITE BENCHMARK    #
############################
EOF
    time write_data "lineitem" "pxf_lineitem_write"
    cat << EOF
Validating data
---------------
EOF
    pxf_validate_write_to_external
}

function run_gphdfs_benchmark {
    create_gphdfs_external_tables

    cat << EOF


############################
#  GPHDFS READ BENCHMARK   #
############################
EOF
    time psql -c "SELECT COUNT(*) FROM gphdfs_lineitem_read"

    cat << EOF


############################
#  GPHDFS WRITE BENCHMARK  #
############################
EOF
    time write_data "lineitem" "gphdfs_lineitem_write"
    cat << EOF
Validating data
---------------
EOF
    gphdfs_validate_write_to_external
}

function create_s3_extension_external_tables {
    psql -c "CREATE EXTERNAL TABLE lineitem_s3_c (like lineitem)
        location('s3://s3.us-west-2.amazonaws.com/gpdb-ud-scratch/s3-profile-test/lineitem/10/ config=/home/gpadmin/s3/s3.conf') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE EXTERNAL TABLE lineitem_s3_pxf (like lineitem)
        location('pxf://s3-profile-test/lineitem/10/?PROFILE=HdfsTextSimple') format 'CSV' (DELIMITER '|');"

    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_s3_c_write (like lineitem)
        LOCATION('s3://s3.us-west-2.amazonaws.com/gpdb-ud-scratch/s3-profile-test/output/ config=/home/gpadmin/s3/s3.conf') FORMAT 'CSV'"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_s3_pxf_write (LIKE lineitem)
        LOCATION('pxf://s3-profile-test/output/?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
}

function assert_count_in_table {
    local table_name="$1"
    local expected_count="$2"

    local num_rows=$(time psql -t -c "SELECT COUNT(*) FROM $table_name" | tr -d ' ')

    if [ ${num_rows} != ${expected_count} ]; then
        echo "Expected number of rows to be ${expected_count} but was ${num_rows}"
        exit 1
    fi
}

function run_s3_extension_benchmark {
    create_s3_extension_external_tables

    cat << EOF


############################
# S3 C Ext READ BENCHMARK  #
############################
EOF

    assert_count_in_table "lineitem_s3_c" "${LINEITEM_COUNT}"

    cat << EOF


############################
# S3 C Ext WRITE BENCHMARK #
############################
EOF
    time psql -c "INSERT INTO lineitem_s3_c_write SELECT * FROM lineitem"

    # We need to update core-site.xml to point to the the S3 bucket
    # and we need to provide AWS credentials

    cat > /tmp/core-site.xml <<-EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
      <name>fs.defaultFS</name>
      <value>s3a://gpdb-ud-scratch</value>
    </property>
    <property>
      <name>fs.s3a.access.key</name>
      <value>${AWS_ACCESS_KEY_ID}</value>
    </property>
    <property>
      <name>fs.s3a.secret.key</name>
      <value>${AWS_SECRET_ACCESS_KEY}</value>
    </property>
    <property>
      <name>fs.s3a.fast.upload</name>
      <value>true</value>
    </property>
</configuration>
EOF

    # Make a backup of core-site and update it with the S3 core-site
    gpscp -u gpadmin -f /tmp/segment_hosts /tmp/core-site.xml =:/tmp/core-site-patch.xml
    gpssh -u gpadmin -f /tmp/segment_hosts -v -s -e \
      'source /usr/local/greenplum-db-devel/greenplum_path.sh && mv $GPHOME/pxf/conf/core-site.xml $GPHOME/pxf/conf/core-site.xml.back && cp /tmp/core-site-patch.xml $GPHOME/pxf/conf/core-site.xml'

    cat << EOF


############################
#  S3 PXF READ BENCHMARK   #
############################
EOF
    assert_count_in_table "lineitem_s3_pxf" "${LINEITEM_COUNT}"

    cat << EOF


############################
#  S3 PXF WRITE BENCHMARK  #
############################
EOF
    time psql -c "INSERT INTO lineitem_s3_pxf_write SELECT * FROM lineitem"

    # Restore core-site
    gpssh -u gpadmin -f /tmp/segment_hosts -v -s -e \
      'source /usr/local/greenplum-db-devel/greenplum_path.sh && mv $GPHOME/pxf/conf/core-site.xml $GPHOME/pxf/conf/core-site.xml.s3 && cp $GPHOME/pxf/conf/core-site.xml.back $GPHOME/pxf/conf/core-site.xml && $GPHOME/pxf/bin/pxf restart'
}

function main {
    setup_gpadmin_user
    setup_sshd
    remote_access_to_gpdb
    install_gpdb_binary

    source ${GPHOME}/greenplum_path.sh
    create_database_and_schema

    echo "Loading data from external into GPDB..."
    write_data "lineitem_external" "lineitem"
    echo "Validating loaded data..."
    validate_write_to_gpdb "lineitem_external" "lineitem"
    echo -e "Data loading and validation complete\n"
    LINEITEM_COUNT=$(psql -t -c "SELECT COUNT(*) FROM lineitem" | tr -d ' ')

#    run_s3_extension_benchmark

    if [ "${BENCHMARK_GPHDFS}" == "true" ]; then
        run_gphdfs_benchmark
    fi
    run_pxf_benchmark
}

main
