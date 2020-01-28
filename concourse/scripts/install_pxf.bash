#!/bin/bash

set -euxo pipefail

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
GPHOME=/usr/local/greenplum-db-devel
# we need word boundary in case of standby master (smdw)
MASTER_HOSTNAME=$(grep < cluster_env_files/etc_hostfile '\bmdw' | awk '{print $2}')
REALM=${REALM:-}
REALM_2=${REALM_2:-}
GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
GOOGLE_ZONE=${GOOGLE_ZONE:-us-central1-a}
KERBEROS=${KERBEROS:-false}
if [[ -f terraform_dataproc/name ]]; then
	HADOOP_HOSTNAME=ccp-$(< terraform_dataproc/name)-m
	HADOOP_IP=$(getent hosts "${HADOOP_HOSTNAME}.${GOOGLE_ZONE}.c.${GOOGLE_PROJECT_ID}.internal" | awk '{ print $1 }')
elif [[ -f dataproc_env_files/name ]]; then
	HADOOP_HOSTNAME=$(< dataproc_env_files/name)
	HADOOP_IP=$(getent hosts "${HADOOP_HOSTNAME}" | awk '{ print $1 }')
	REALM=$(< dataproc_env_files/REALM)
else
	HADOOP_HOSTNAME=hadoop
	HADOOP_IP=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
fi
PROXY_USER=${PROXY_USER:-pxfuser}
PXF_CONF_DIR=~gpadmin/pxf
INSTALL_GPHDFS=${INSTALL_GPHDFS:-true}

cat << EOF
	############################
	#                          #
	#     PXF Installation     #
	#                          #
	############################
EOF

function create_pxf_installer_scripts() {
	cat > /tmp/configure_pxf.sh <<-EOFF
		#!/bin/bash

		set -euxo pipefail

		GPHOME=/usr/local/greenplum-db-devel
		PXF_HOME=\${GPHOME}/pxf
		PXF_CONF=${PXF_CONF_DIR}

		function setup_pxf_env() {
		  #Check if some other process is listening on 5888
		  netstat -tlpna | grep 5888 || true

		  if [[ $IMPERSONATION == false ]]; then
		    echo 'Impersonation is disabled, updating pxf-env.sh property'
		    su gpadmin -c "
		      sed -ie 's|^[[:blank:]]*export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g' \
		        "\${PXF_CONF}/conf/pxf-env.sh"
		    "
		  fi

		  if [[ -n "${PXF_JVM_OPTS}" ]]; then
		    echo 'export PXF_JVM_OPTS="${PXF_JVM_OPTS}"' >> "\${PXF_CONF}/conf/pxf-env.sh"
		  fi

		  if [[ $KERBEROS == true ]]; then

		    cp ~/dataproc_env_files/krb5.conf /tmp/krb5.conf

		    if [[ -f ~/dataproc_2_env_files/krb5.conf ]]; then
		      # Merge krb5.conf files from two different REALMS
		      diff --line-format %L /tmp/krb5.conf ~/dataproc_2_env_files/krb5.conf  > /tmp/krb5.conf-tmp || true
		      rm -f /tmp/krb5.conf && mv /tmp/krb5.conf-tmp /tmp/krb5.conf
		      # Remove the second instance of default_realm from the file
		      awk '!/default_realm/ || !f++' /tmp/krb5.conf > /tmp/krb5.conf-tmp
		      rm -f /tmp/krb5.conf && mv /tmp/krb5.conf-tmp /tmp/krb5.conf
		      # Add missing } to the new REALM
		      REALM_2=\$(cat ~/dataproc_2_env_files/REALM)
		      sed -i "s/\${REALM_2} =/}\n\t\${REALM_2} =/g" /tmp/krb5.conf
		    fi

		    echo 'export PXF_KEYTAB="\${PXF_CONF}/keytabs/pxf.service.keytab"' >> "\${PXF_CONF}/conf/pxf-env.sh"
		    echo 'export PXF_PRINCIPAL="gpadmin@${REALM}"' >> "\${PXF_CONF}/conf/pxf-env.sh"
		    gpscp -f ~gpadmin/hostfile_all -v -r -u gpadmin ~/dataproc_env_files/pxf.service.keytab =:/home/gpadmin/pxf/keytabs/
		    gpscp -f ~gpadmin/hostfile_all -v -r -u centos /tmp/krb5.conf =:/tmp/krb5.conf
		    gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo mv /tmp/krb5.conf /etc/krb5.conf'
		  fi
		}

		function main() {
		  rm -rf \$PXF_CONF/servers/default/*-site.xml
		  if [[ -d ~/dataproc_env_files/conf ]]; then
		    cp ~/dataproc_env_files/conf/*-site.xml "\$PXF_CONF/servers/default"
		    # required for recursive directories tests
		    cp "\$PXF_CONF/templates/mapred-site.xml" "\$PXF_CONF/servers/default/mapred1-site.xml"
		  else
		    cp \$PXF_CONF/templates/{hdfs,mapred,yarn,core,hbase,hive,pxf}-site.xml "\$PXF_CONF/servers/default"
		    sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${HADOOP_IP}/g' \$PXF_CONF/servers/default/*-site.xml
		    sed -i -e 's|\${user.name}|${PROXY_USER}|g' \$PXF_CONF/servers/default/pxf-site.xml
		  fi
		  setup_pxf_env
		}

		main

	EOFF

	cat > /tmp/install_pxf_dependencies.sh <<-EOFF
		#!/bin/bash

		set -euxo pipefail

		GPHOME=/usr/local/greenplum-db-devel
		PXF_HOME=\${GPHOME}/pxf
		PXF_CONF=${PXF_CONF_DIR}
		export HADOOP_VER=2.6.5.0-292

		function install_java() {
		  yum install -y -d 1 java-1.8.0-openjdk-devel java-1.8.0-openjdk-devel-debug
		  echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bashrc
		  echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bashrc
		}

		function install_hadoop_client() {
		  cat > /etc/yum.repos.d/hdp.repo <<EOF
		[HDP-2.6.5.0]
		name=HDP Version - HDP-2.6.5.0
		baseurl=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0
		gpgcheck=1
		gpgkey=http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
		enabled=1
		priority=1
		EOF
		  yum install -y -d 1 hadoop-client
		  echo "export HADOOP_VERSION=\${HADOOP_VER}" | sudo tee -a ~gpadmin/.bash_profile
		  echo "export HADOOP_HOME=/usr/hdp/\${HADOOP_VER}" | sudo tee -a ~gpadmin/.bash_profile
		  echo "export HADOOP_HOME=/usr/hdp/\${HADOOP_VER}" | sudo tee -a ~centos/.bash_profile
		}

		function main() {
		  install_java
		  if [[ $INSTALL_GPHDFS == true ]]; then
		    install_hadoop_client
		  fi
		}

		main
	EOFF

	chmod +x /tmp/{install_pxf_dependencies,configure_pxf}.sh
	scp /tmp/{install_pxf_dependencies,configure_pxf}.sh "${MASTER_HOSTNAME}:~gpadmin"
}

function run_pxf_installer_scripts() {
	ssh "${MASTER_HOSTNAME}" "
		source ${GPHOME}/greenplum_path.sh &&
		export JAVA_HOME=/usr/lib/jvm/jre &&
		export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1/ &&
		if [[ $INSTALL_GPHDFS == true ]]; then
			gpconfig -c gp_hadoop_home -v '/usr/hdp/2.6.5.0-292'
			gpconfig -c gp_hadoop_target_version -v hdp
			gpstop -u
		fi &&
		sed -i '/edw/d' hostfile_all &&
		gpscp -f ~gpadmin/hostfile_all -v -u centos ~gpadmin/install_pxf_dependencies.sh centos@=: &&
		gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e 'sudo ~centos/install_pxf_dependencies.sh' &&
		gpscp -f ~gpadmin/hostfile_all -v -u gpadmin -r ~/pxf_tarball gpadmin@=: &&
		gpssh -f ~gpadmin/hostfile_all -v -u gpadmin -s -e 'tar -xzf ~/pxf_tarball/pxf.tar.gz -C ${GPHOME}' &&
		PXF_CONF=${PXF_CONF_DIR} ${GPHOME}/pxf/bin/pxf cluster init &&
		if [[ -d ~/dataproc_env_files ]]; then
			gpscp -f ~gpadmin/hostfile_init -v -r -u gpadmin ~/dataproc_env_files =:
		fi &&
		~gpadmin/configure_pxf.sh &&
		gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e \"sudo sed -i -e 's/edw0/edw0 hadoop/' /etc/hosts\" &&
		${GPHOME}/pxf/bin/pxf cluster sync &&
		${GPHOME}/pxf/bin/pxf cluster start &&
		if [[ $INSTALL_GPHDFS == true ]]; then
			gpssh -f ~gpadmin/hostfile_all -v -u centos -s -e '
				sudo cp ${PXF_CONF_DIR}/servers/default/{core,hdfs}-site.xml /etc/hadoop/conf
			'
		fi
	"
}

function _main() {
	local SCP_FILES=(pxf_tarball cluster_env_files/*)

	if [[ -d dataproc_env_files ]]; then
		SCP_FILES+=(dataproc_env_files)
	fi

	if [[ -d dataproc_2_env_files ]]; then
		SCP_FILES+=(dataproc_2_env_files)
	fi

	scp -r "${SCP_FILES[@]}" "${MASTER_HOSTNAME}:~gpadmin"

	create_pxf_installer_scripts
	run_pxf_installer_scripts
}

_main
