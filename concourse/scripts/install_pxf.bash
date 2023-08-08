#!/bin/bash

set -euxo pipefail

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

GPHOME=/usr/local/greenplum-db-devel
# whether PXF is being installed from a new component-based packaging
PXF_COMPONENT=${PXF_COMPONENT:=false}
if [[ ${PXF_COMPONENT} == "true" ]]; then
	PXF_HOME=/usr/local/pxf-gp${GP_VER}
else
	PXF_HOME=${GPHOME}/pxf
fi

# gpscp command is replaced with gpsync in GP7
if [ "${GP_VER}" -lt 7 ]; then
  GP_SCP_CMD=gpscp
else
  GP_SCP_CMD=gpsync
fi

PXF_VERSION=${PXF_VERSION:=6}
# read default user from terraform metadata file
DEFAULT_OS_USER=$(jq --raw-output ".ami_default_user" terraform/metadata)


# we need word boundary in case of standby coordinator (scdw)
COORDINATOR_HOSTNAME="cdw"
REALM=${REALM:-}
REALM_2=${REALM_2:-}
GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID:-data-gpdb-ud}
KERBEROS=${KERBEROS:-false}
function get_hadoop_ip() {
	if [[ $SKIP_HADOOP_SETUP == true ]]; then
		HADOOP_IP=''
		return
	fi
	if [[ -f terraform_dataproc/name ]]; then
		HADOOP_HOSTNAME=ccp-$(< terraform_dataproc/name)-m
		HADOOP_IP=$(getent hosts "${HADOOP_HOSTNAME}.c.${GOOGLE_PROJECT_ID}.internal" | awk '{ print $1 }')
	elif [[ -f dataproc_env_files/name ]]; then
		HADOOP_HOSTNAME=$(< dataproc_env_files/name)
		HADOOP_IP=$(getent hosts "${HADOOP_HOSTNAME}.c.${GOOGLE_PROJECT_ID}.internal" | awk '{ print $1 }')
		REALM=$(< dataproc_env_files/REALM)
	else
		HADOOP_HOSTNAME=hadoop
		HADOOP_IP=$(grep < cluster_env_files/etc_hostfile edw0 | awk '{print $1}')
	fi
}
SKIP_HADOOP_SETUP=${SKIP_HADOOP_SETUP:-false}
get_hadoop_ip

PROXY_USER=${PROXY_USER:-pxfuser}
if [[ ${PXF_VERSION} == 5 ]]; then
	BASE_DIR=~gpadmin/pxf
else
	BASE_DIR=${PXF_BASE_DIR:-$PXF_HOME}
fi
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

		GPHOME=${GPHOME}
		PXF_HOME=${PXF_HOME}
		if [[ ${PXF_VERSION} == 5 ]]; then
		  PXF_CONF=${BASE_DIR}
		else
		  PXF_BASE=${BASE_DIR}
		fi

		function setup_pxf_env() {
		  #Check if some other process is listening on 5888
		  netstat -tlpna | grep 5888 || true

		  if [[ $IMPERSONATION == false ]]; then
		    if [[ ${PXF_VERSION} == 5 ]]; then
		      echo 'Impersonation is disabled, updating pxf-env.sh property'
		      # sed -ie 's|^[[:blank:]]*export PXF_USER_IMPERSONATION=.*$|export PXF_USER_IMPERSONATION=false|g' "\${PXF_CONF}/conf/pxf-env.sh"
		      echo 'PXF_USER_IMPERSONATION=false' >> "\${PXF_CONF}/conf/pxf-env.sh"
		    else
		      echo 'Impersonation is disabled, updating pxf-site.xml property'
		      if [[ ! -f \${PXF_BASE}/servers/default/pxf-site.xml ]]; then
		        cp \${PXF_HOME}/templates/pxf-site.xml \${PXF_BASE}/servers/default/pxf-site.xml
		      fi
		      sed -i -e "s|<value>true</value>|<value>false</value>|g" \${PXF_BASE}/servers/default/pxf-site.xml
		    fi
		  fi

		  if [[ -n "${PXF_JVM_OPTS}" ]]; then
		    if [[ ${PXF_VERSION} == 5 ]]; then
		      echo 'export PXF_JVM_OPTS="${PXF_JVM_OPTS}"' >> "\${PXF_CONF}/conf/pxf-env.sh"
		    else
		      echo 'export PXF_JVM_OPTS="${PXF_JVM_OPTS}"' >> "\${PXF_BASE}/conf/pxf-env.sh"
		    fi
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

			if [[ -d ~/ipa_env_files ]]; then
				REALM_3="\$(< ipa_env_files/REALM)"
				sed -i \
					-e '/^\[libdefaults.*/a  \\\\tforwardable=true' \
					-e '/^\[realms/ r ipa_env_files/krb5_realm' \
					-e '/^\[domain_realm/ r ipa_env_files/krb5_domain_realm' /tmp/krb5.conf
			fi

		    if [[ ${PXF_VERSION} == 5 ]]; then
		      echo 'export PXF_KEYTAB="\${PXF_CONF}/keytabs/pxf.service.keytab"' >> "\${PXF_CONF}/conf/pxf-env.sh"
		      echo 'export PXF_PRINCIPAL="gpadmin@${REALM}"' >> "\${PXF_CONF}/conf/pxf-env.sh"
		      ${GP_SCP_CMD} -f ~gpadmin/hostfile_all -v -r -u gpadmin ~/dataproc_env_files/pxf.service.keytab =:/home/gpadmin/pxf/keytabs/
		    else
		      if [[ ! -f \${PXF_BASE}/servers/default/pxf-site.xml ]]; then
		        cp \${PXF_HOME}/templates/pxf-site.xml \${PXF_BASE}/servers/default/pxf-site.xml
		      fi

		      sed -i -e "s|gpadmin/_HOST@EXAMPLE.COM|gpadmin@${REALM}|g" ${BASE_DIR}/servers/default/pxf-site.xml
		      ${GP_SCP_CMD} -f ~gpadmin/hostfile_all -v -r -u gpadmin ~/dataproc_env_files/pxf.service.keytab =:${BASE_DIR}/keytabs/
		    fi
		    ${GP_SCP_CMD} -f ~gpadmin/hostfile_all -v -r -u ${DEFAULT_OS_USER} /tmp/krb5.conf =:/tmp/krb5.conf
		    gpssh -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -s -e 'sudo mv /tmp/krb5.conf /etc/krb5.conf'
		  fi
		}

		function main() {
		  if [[ ${PXF_VERSION} == 5 ]]; then
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
		  else
		    if [[ "\$PXF_BASE" != "\$PXF_HOME" ]]; then
		      echo 'Prepare PXF in $BASE_DIR'
		      PXF_BASE=\$PXF_BASE \$PXF_HOME/bin/pxf cluster prepare
		      echo "export PXF_BASE=${BASE_DIR}" >> ~gpadmin/.bashrc
		    fi
		    rm -rf \$PXF_BASE/servers/default/*-site.xml
		    if [[ -d ~/dataproc_env_files/conf ]]; then
		      cp ~/dataproc_env_files/conf/*-site.xml "\$PXF_BASE/servers/default"
		      # required for recursive directories tests
		      cp "\$PXF_HOME/templates/mapred-site.xml" "\$PXF_BASE/servers/default/mapred1-site.xml"
		    else
		      cp \$PXF_HOME/templates/{hdfs,mapred,yarn,core,hbase,hive,pxf}-site.xml "\$PXF_BASE/servers/default"
		      sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${HADOOP_IP}/g' \$PXF_BASE/servers/default/*-site.xml
		      sed -i -e 's|\${user.name}|${PROXY_USER}|g' \$PXF_BASE/servers/default/pxf-site.xml
		    fi
		  fi
		  setup_pxf_env
		}

		main

	EOFF

	cat > /tmp/install_pxf_dependencies.sh <<-EOFF
		#!/bin/bash

		set -euxo pipefail

		GPHOME=${GPHOME}
		PXF_HOME=${PXF_HOME}
		if [[ ${PXF_VERSION} == 5 ]]; then
		  PXF_CONF=${BASE_DIR}
		else
		  PXF_BASE=${BASE_DIR}
		fi
		export HADOOP_VER=2.6.5.0-292

		function install_java() {
		  yum install -y -q -e 0 java-1.8.0-openjdk
		  echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bashrc
		  echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~${DEFAULT_OS_USER}/.bashrc
		}

		# this function is only used for GPHDFS, which we only test with centos.
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
		  echo "export HADOOP_HOME=/usr/hdp/\${HADOOP_VER}" | sudo tee -a ~${DEFAULT_OS_USER}/.bash_profile
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
	scp /tmp/{install_pxf_dependencies,configure_pxf}.sh "${COORDINATOR_HOSTNAME}:~gpadmin"
}

function run_pxf_installer_scripts() {
	ssh "${COORDINATOR_HOSTNAME}" "
		source ${GPHOME}/greenplum_path.sh &&
		export JAVA_HOME=/usr/lib/jvm/jre &&
		export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1/ &&
		export PXF_COMPONENT=${PXF_COMPONENT} &&
		if [[ $INSTALL_GPHDFS == true ]]; then
			gpconfig -c gp_hadoop_home -v '/usr/hdp/2.6.5.0-292'
			gpconfig -c gp_hadoop_target_version -v hdp
			gpstop -u
		fi &&
		sed -i '/edw/d' hostfile_all &&
		${GP_SCP_CMD} -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -r ~/pxf_installer ~gpadmin/install_pxf_dependencies.sh ${DEFAULT_OS_USER}@=: &&
		gpssh -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -s -e 'sudo ~${DEFAULT_OS_USER}/install_pxf_dependencies.sh' &&
		gpssh -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -s -e 'sudo GPHOME=${GPHOME} ~${DEFAULT_OS_USER}/pxf_installer/install_component'
		gpssh -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -s -e 'sudo chown -R gpadmin:gpadmin ${PXF_HOME}'
		if [[ ${PXF_VERSION} == 5 ]]; then
			GPHOME=${GPHOME} PXF_CONF=${BASE_DIR} ${PXF_HOME}/bin/pxf cluster init
		else
			${PXF_HOME}/bin/pxf cluster register
		fi &&
		if [[ -d ~/dataproc_env_files ]]; then
			${GP_SCP_CMD} -f ~gpadmin/hostfile_init -v -r -u gpadmin ~/dataproc_env_files =:
		fi &&
		~gpadmin/configure_pxf.sh &&
		source ~gpadmin/.bashrc &&
		gpssh -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -s -e \"sudo sed -i -e 's/edw0/edw0 hadoop/' /etc/hosts\" &&
		echo \"PXF_BASE=\${PXF_BASE}\" &&
		${PXF_HOME}/bin/pxf cluster sync &&
		${PXF_HOME}/bin/pxf cluster start &&
		if [[ $INSTALL_GPHDFS == true ]]; then
			gpssh -f ~gpadmin/hostfile_all -v -u ${DEFAULT_OS_USER} -s -e '
				sudo cp ${BASE_DIR}/servers/default/{core,hdfs}-site.xml /etc/hadoop/conf
			'
		fi
	"

	# Create a database for PXF extension upgrade testing
	if [[ ${PXF_VERSION} == 5 ]]; then
		ssh "${COORDINATOR_HOSTNAME}" "
			source ${GPHOME}/greenplum_path.sh &&
			createdb testupgrade &&
			psql -d testupgrade -c 'CREATE EXTENSION IF NOT EXISTS pxf'
		"
	fi
}

function _main() {
	mkdir -p /tmp/pxf_installer/
	if [[ -d pxf_tarball ]]; then
		if [[ ${PXF_COMPONENT} == true ]]; then
			mkdir -p /tmp/pxf_inflate
			tar -xzf pxf_tarball/pxf-*.tar.gz -C /tmp/pxf_inflate
			cp /tmp/pxf_inflate/pxf*/* /tmp/pxf_installer/
		else
			cp pxf_tarball/pxf.tar.gz /tmp/pxf_installer
			cat << EOF > /tmp/pxf_installer/install_component
#!/usr/bin/env bash

tar -xzf ~/pxf_tarball/pxf.tar.gz -C \${GPHOME}
EOF
		fi
	elif [[ -d pxf_artifact ]]; then
		cp pxf_artifact/*.rpm /tmp/pxf_installer
		cp pxf_src/package/install_rpm /tmp/pxf_installer/install_component
	else
		echo "Unable to find a suitable PXF installer"
		exit 1
	fi
	chmod +x /tmp/pxf_installer/install_component

	local SCP_FILES=(/tmp/pxf_installer cluster_env_files/*)

	if [[ -d dataproc_env_files ]]; then
		SCP_FILES+=(dataproc_env_files)
	fi

	if [[ -d dataproc_2_env_files ]]; then
		SCP_FILES+=(dataproc_2_env_files)
	fi

	if [[ -d ipa_env_files ]]; then
		SCP_FILES+=(ipa_env_files)
	fi

	scp -r "${SCP_FILES[@]}" "${COORDINATOR_HOSTNAME}:~gpadmin"

	create_pxf_installer_scripts
	run_pxf_installer_scripts
}

_main
