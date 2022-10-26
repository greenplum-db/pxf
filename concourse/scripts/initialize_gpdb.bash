#!/usr/bin/env bash

set -exuo pipefail

GPHOME=${GPHOME:=/usr/local/greenplum-db}
PYTHONHOME='' source "${GPHOME}/greenplum_path.sh"

# Create config and data dirs.
data_dirs=(~gpadmin/data{1..3}/primary)
dirs=(~gpadmin/{gpconfigs,data/master} "${data_dirs[@]}")
mkdir -p "${dirs[@]}"

# cdw is the new name for the coordinator host (previously master)
sed -e "s/MASTER_HOSTNAME=mdw/MASTER_HOSTNAME=\$(hostname -f)/g" \
	-e "s/COORDINATOR_HOSTNAME=cdw/COORDINATOR_HOSTNAME=\$(hostname -f)/g" \
	-e "s|declare -a DATA_DIRECTORY.*|declare -a DATA_DIRECTORY=( ${data_dirs[*]} )|g" \
	-e "s|MASTER_DIRECTORY=.*|MASTER_DIRECTORY=~gpadmin/data/master|g" \
	-e "s|COORDINATOR_DIRECTORY=.*|COORDINATOR_DIRECTORY=~gpadmin/data/master|g" \
	-e "s|MASTER_PORT=.*|MASTER_PORT=${PGPORT:-5432}|g" \
	-e "s|COORDINATOR_PORT=.*|COORDINATOR_PORT=${PGPORT:-5432}|g" \
	"${GPHOME}/docs/cli_help/gpconfigs/gpinitsystem_config" >~gpadmin/gpconfigs/gpinitsystem_config
chmod +w ~gpadmin/gpconfigs/gpinitsystem_config

#Script to start segments and create directories.
hostname -f >/tmp/hosts.txt

# gpinitsystem fails in concourse environment without this "ping" workaround. "[FATAL]:-Unknown host..."
sudo chmod u+s /bin/ping

pgrep sshd || sudo /usr/sbin/sshd
gpssh-exkeys -f /tmp/hosts.txt

# 5X gpinitsystem returns 1 exit code on warnings.
# so we ignore return code of 1, but otherwise we fail
set +e
gpinitsystem -a -c ~gpadmin/gpconfigs/gpinitsystem_config -h /tmp/hosts.txt --su_password=changeme
(( $? > 1 )) && exit 1
set -e

echo 'host all all 0.0.0.0/0 password' >>~gpadmin/data/master/gpseg-1/pg_hba.conf

# reload pg_hba.conf
MASTER_DATA_DIRECTORY=~gpadmin/data/master/gpseg-1 gpstop -u

sleep 3
psql -d template1 -c "CREATE DATABASE gpadmin;"
