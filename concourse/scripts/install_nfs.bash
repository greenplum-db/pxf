#!/bin/bash

set -euxo pipefail

GPHOME=/usr/local/greenplum-db-devel
# we need word boundary in case of standby coordinator (scdw)
COORDINATOR_HOSTNAME="cdw"
BASE_PATH=${BASE_PATH:-/mnt/nfs/var/nfsshare}

cat << EOF
  ############################
  #                          #
  #     NFS Installation     #
  #                          #
  ############################
EOF

function create_nfs_installer_scripts() {
  cat > /tmp/install_and_configure_nfs_client.sh <<-EOFF
#!/bin/bash

set -euxo pipefail

echo "check available NFS shares in cdw"
showmount -e cdw

echo "create mount point and mount it"
mkdir -p ${BASE_PATH}
mount -t nfs cdw:/var/nfs ${BASE_PATH}
chown gpadmin:gpadmin ${BASE_PATH}
chmod 755 ${BASE_PATH}

echo "verify the mount worked"
mount | grep nfs
df -hT

echo "write a test file to make sure it worked"
sudo runuser -l gpadmin -c "touch ${BASE_PATH}/$(hostname)-test"
ls -l ${BASE_PATH}

EOFF

  chmod +x /tmp/install_and_configure_nfs_client.sh
  scp /tmp/install_and_configure_nfs_client.sh "${COORDINATOR_HOSTNAME}:~gpadmin"
}

# assumes only two segment hosts sdw1 and sdw2
function run_nfs_installation() {

  # install and configure the NFS server on coordinator
  ssh "centos@${COORDINATOR_HOSTNAME}" "
    echo 'enable and start the NFS service'
    sudo systemctl enable nfs-server rpcbind
    sudo systemctl start nfs-server rpcbind
    echo 'create a shared directory'
    sudo mkdir -p /var/nfs
    sudo chown nfsnobody:nfsnobody /var/nfs
    sudo chmod 755 /var/nfs
    echo 'add /var/nfs to the exports file'
    sudo sh -c \"echo '/var/nfs sdw1(rw,sync,no_root_squash,no_subtree_check) sdw2(rw,sync,no_root_squash,no_subtree_check) 10.0.80.0/23(rw,sync,no_root_squash,no_subtree_check)' >> /etc/exports\"
    echo 'ensure the file was modified'
    cat /etc/exports
    echo 'export shared directories'
    sudo exportfs -r
    echo 'display available shares on this server'
    sudo showmount -e ${COORDINATOR_HOSTNAME}
  "

  # install and configure the NFS clients on sdw1 and sdw2
  ssh "${COORDINATOR_HOSTNAME}" "
    source ${GPHOME}/greenplum_path.sh &&
    gpscp -f ~gpadmin/hostfile_init -v -u centos ~gpadmin/install_and_configure_nfs_client.sh centos@=: &&
    gpssh -f ~gpadmin/hostfile_init -v -u centos -s -e 'sudo ~centos/install_and_configure_nfs_client.sh'
  "
}

function _main() {
  local SCP_FILES=(cluster_env_files/hostfile_init)
  scp -r "${SCP_FILES[@]}" "${COORDINATOR_HOSTNAME}:~gpadmin"

  create_nfs_installer_scripts
  run_nfs_installation
}

_main
