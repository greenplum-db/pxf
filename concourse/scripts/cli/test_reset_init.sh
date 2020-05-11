#!/usr/bin/env bash

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
[[ -e ${dir}/common.sh ]] || exit 1
source "${dir}/common.sh"

# all these things come from pxf init, removed by pxf reset
pxf_home_contents="${GPHOME}/pxf/conf/pxf-private.classpath
${GPHOME}/pxf/run
${GPHOME}/pxf/pxf-service/conf/logging.properties
${GPHOME}/pxf/pxf-service/conf/server.xml
${GPHOME}/pxf/pxf-service/conf/web.xml
${GPHOME}/pxf/pxf-service/lib
${GPHOME}/pxf/pxf-service/webapps/pxf.war
${GPHOME}/pxf/pxf-service/bin/catalina.sh
${GPHOME}/pxf/pxf-service/bin/kill-pxf.sh
${GPHOME}/pxf/pxf-service/bin/setenv.sh"
pxf_home_empty="export PXF_CONF=\${PXF_CONF:-NOT_INITIALIZED}"
list_pxf_home() {
	local usage='list_pxf_home <host>' host=${1:?${usage}}
	ssh "${host}" "
		[[ -e $GPHOME/pxf/conf/pxf-private.classpath ]] && ls $GPHOME/pxf/conf/pxf-private.classpath
		[[ -d $GPHOME/pxf/run ]] && echo $GPHOME/pxf/run
		[[ -d $GPHOME/pxf/pxf-service/conf ]] && ls $GPHOME/pxf/pxf-service/conf/{logging.properties,{server,web}.xml}
		[[ -d $GPHOME/pxf/pxf-service/lib ]] && echo $GPHOME/pxf/pxf-service/lib
		[[ -d $GPHOME/pxf/pxf-service/webapps ]] && ls $GPHOME/pxf/pxf-service/webapps/pxf.war
		[[ -d $GPHOME/pxf/pxf-service/bin ]] && ls $GPHOME/pxf/pxf-service/bin/{catalina,kill-pxf,setenv}.sh
		grep NOT_INITIALIZED ${GPHOME}/pxf/conf/pxf-env-default.sh
	"
}

# all these things come from pxf init, but not removed by pxf reset
pxf_conf_contents="${PXF_CONF}/conf/pxf-env.sh
${PXF_CONF}/conf/pxf-log4j2.xml
${PXF_CONF}/conf/pxf-profiles.xml
${PXF_CONF}/lib
${PXF_CONF}/logs
${PXF_CONF}/keytabs
${PXF_CONF}/servers/default
${PXF_CONF}/templates/adl-site.xml
${PXF_CONF}/templates/core-site.xml
${PXF_CONF}/templates/gs-site.xml
${PXF_CONF}/templates/hbase-site.xml
${PXF_CONF}/templates/hdfs-site.xml
${PXF_CONF}/templates/hive-site.xml
${PXF_CONF}/templates/jdbc-site.xml
${PXF_CONF}/templates/mapred-site.xml
${PXF_CONF}/templates/minio-site.xml
${PXF_CONF}/templates/pxf-site.xml
${PXF_CONF}/templates/s3-site.xml
${PXF_CONF}/templates/wasbs-site.xml
${PXF_CONF}/templates/yarn-site.xml
JAVA_HOME=/usr/lib/jvm/jre"
list_pxf_conf() {
	local usage='list_pxf_conf <host>' host=${1:?${usage}}
	ssh "${host}" "
		[[ -d $PXF_CONF/conf ]] && ls $PXF_CONF/conf/pxf-{env.sh,log4j.properties,profiles.xml}
		[[ -d $PXF_CONF/lib ]] && echo $PXF_CONF/lib
		[[ -d $PXF_CONF/logs ]] && echo $PXF_CONF/logs
		[[ -d $PXF_CONF/keytabs ]] && echo $PXF_CONF/keytabs
		[[ -d $PXF_CONF/servers/default ]] && echo $PXF_CONF/servers/default
		[[ -d $PXF_CONF/templates ]] && ls $PXF_CONF/templates/{adl,core,gs,hbase,hdfs,hive,jdbc,mapred,minio,pxf,s3,wasbs,yarn}-site.xml
		grep '^JAVA_HOME=.*' ${PXF_CONF}/conf/pxf-env.sh
	"
}

for host in {s,}mdw sdw{1,2}; do
	compare "${pxf_home_contents}" "$(list_pxf_home "${host}")" "${host} should have \$PXF_HOME populated"
	compare "${pxf_conf_contents}" "$(list_pxf_conf "${host}")" "${host} should have \$PXF_CONF populated"
done

expected_reset_err_message="Ensure your PXF cluster is stopped before continuing. This is a destructive action. Press y to continue:
Resetting PXF on master host, standby master host, and 2 segment hosts...
ERROR: Failed to reset PXF on 2 out of 4 hosts
sdw1 ==> PXF is running. Please stop PXF before running 'pxf [cluster] reset'

sdw2 ==> PXF is running. Please stop PXF before running 'pxf [cluster] reset'"
compare "${expected_reset_err_message}" "$(yes | pxf cluster reset 2>&1)" "pxf cluster reset should fail on sdw{1,2}"

for host in {s,}mdw; do
	compare "${pxf_home_empty}" "$(list_pxf_home "${host}")" "${host} should not have \$PXF_HOME populated"
	compare "${pxf_conf_contents}" "$(list_pxf_conf "${host}")" "${host} should still have \$PXF_CONF populated"
done

for host in sdw{1,2}; do
	compare "${pxf_home_contents}" "$(list_pxf_home "${host}")" "${host} should still have \$PXF_HOME populated"
	compare "${pxf_conf_contents}" "$(list_pxf_conf "${host}")" "${host} should still have \$PXF_CONF populated"
done

successful_stop_message="Stopping PXF on 2 segment hosts...
PXF stopped successfully on 2 out of 2 hosts"
compare "${successful_stop_message}" "$(pxf cluster stop)" "pxf cluster stop should succeed"

failed_init_message="Initializing PXF on master host, standby master host, and 2 segment hosts...
ERROR: PXF failed to initialize on 2 out of 4 hosts
sdw1 ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF

sdw2 ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF"
compare "${failed_init_message}" "$(pxf cluster init 2>&1)" "pxf cluster init should fail on sdw{1,2}"

expected_reset_sdw_message="Cleaning ${GPHOME}/pxf/conf/pxf-private.classpath...
Ignoring ${PXF_CONF}...
Cleaning ${GPHOME}/pxf/pxf-service...
Cleaning ${GPHOME}/pxf/run...
Reverting changes to ${GPHOME}/pxf/conf/pxf-env-default.sh...
Finished cleaning PXF instance directories"
compare "${expected_reset_sdw_message}" "$(ssh sdw1 "${GPHOME}/pxf/bin/pxf" reset --force)" "pxf reset on sdw1 should succeed"
compare "${expected_reset_sdw_message}" "$(ssh sdw2 "${GPHOME}/pxf/bin/pxf" reset --force)" "pxf reset on sdw2 should succeed"

for host in sdw{1,2}; do
	compare "${pxf_home_empty}" "$(list_pxf_home "${host}")" "${host} should not have \$PXF_HOME populated"
	compare "${pxf_conf_contents}" "$(list_pxf_conf "${host}")" "${host} should still have \$PXF_CONF populated"
done

failed_init_message="Initializing PXF on master host, standby master host, and 2 segment hosts...
ERROR: PXF failed to initialize on 2 out of 4 hosts
mdw ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF

smdw ==> Instance already exists. Use 'pxf [cluster] reset' before attempting to re-initialize PXF"
compare "${failed_init_message}" "$(yes | pxf cluster init 2>&1)" "pxf cluster init should fail on {,s}mdw"

for host in {s,}mdw sdw{1,2}; do
	compare "${pxf_home_contents}" "$(list_pxf_home "${host}")" "${host} should have \$PXF_HOME populated once again"
	compare "${pxf_conf_contents}" "$(list_pxf_conf "${host}")" "${host} should have \$PXF_CONF populated once again"
done

successful_start_message="Starting PXF on 2 segment hosts...
PXF started successfully on 2 out of 2 hosts"
compare "${successful_start_message}" "$(pxf cluster start)" "pxf cluster start should succeed"

successful_stop_message="Stopping PXF on 2 segment hosts...
PXF stopped successfully on 2 out of 2 hosts"
successful_reset_message="Ensure your PXF cluster is stopped before continuing. This is a destructive action. Press y to continue:
Resetting PXF on master host, standby master host, and 2 segment hosts...
PXF has been reset on 4 out of 4 hosts"
compare "${successful_stop_message}" "$(pxf cluster stop)" "pxf cluster stop should work"
compare "${successful_reset_message}" "$(yes | pxf cluster reset 2>&1)" "pxf cluster reset should succeed"

# get rid of standby master on smdw
source "${GPHOME}/greenplum_path.sh"
gpinitstandby -ar >/dev/null

failed_init_message="Initializing PXF on master host and 2 segment hosts...
ERROR: PXF failed to initialize on 3 out of 3 hosts
mdw ==> Using ${PXF_CONF} as a location for user-configurable files
ERROR: \$JAVA_HOME=bad_java_home is invalid. Set \$JAVA_HOME in your environment before initializing PXF....
sdw1 ==> Using ${PXF_CONF} as a location for user-configurable files
ERROR: \$JAVA_HOME=bad_java_home is invalid. Set \$JAVA_HOME in your environment before initializing PXF....
sdw2 ==> Using ${PXF_CONF} as a location for user-configurable files
ERROR: \$JAVA_HOME=bad_java_home is invalid. Set \$JAVA_HOME in your environment before initializing PXF...."
compare "${failed_init_message}" "$(yes | JAVA_HOME=bad_java_home pxf cluster init 2>&1)" "pxf cluster init should fail on mdw, sdw{1,2} with bad JAVA_HOME"

successful_init_message="Initializing PXF on master host and 2 segment hosts...
PXF initialized successfully on 3 out of 3 hosts"
# /etc/alternatives/jre is a valid JAVA_HOME; in our CI /usr/lib/jvm/jre is linked there
compare "${successful_init_message}" "$(yes | JAVA_HOME=/etc/alternatives/jre pxf cluster init 2>&1)" "pxf cluster init should succeed on mdw, sdw{1,2} with new JAVA_HOME"

pxf_conf_contents=${pxf_conf_contents/JAVA_HOME=\/usr\/lib\/jvm/JAVA_HOME=\/etc\/alternatives}
for host in mdw sdw{1,2}; do
	compare "${pxf_conf_contents}" "$(list_pxf_conf "${host}")" "${host} should have \$PXF_CONF populated with new JAVA_HOME"
done

gpinitstandby -as smdw >/dev/null

successful_host_init_message="Using ${PXF_CONF} as a location for user-configurable files
Generating ${GPHOME}/pxf/conf/pxf-private.classpath file from ${GPHOME}/pxf/templates/pxf/pxf-private.classpath.template ...
Directory ${PXF_CONF} already exists, no update required
Directory ${PXF_CONF}/conf already exists, no update required
Directory ${PXF_CONF}/keytabs already exists, no update required
Directory ${PXF_CONF}/lib already exists, no update required
Directory ${PXF_CONF}/servers/default already exists, no update required
Updating configurations from ${GPHOME}/pxf/templates/user/templates to ${PXF_CONF}/templates ...
Creating PXF runtime directory ${GPHOME}/pxf/run ..."

compare "${successful_host_init_message}" "$(ssh smdw "PXF_CONF=${PXF_CONF} ${GPHOME}/pxf/bin/pxf init")" "pxf init should succeed on smdw"
compare "${successful_start_message}" "$(pxf cluster start)" "pxf cluster start should succeed"

exit_with_err "${BASH_SOURCE[0]}"
