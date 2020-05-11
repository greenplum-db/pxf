#!/usr/bin/env bash

############################
# dov.dorin at gopivotal.com
############################

usage="Usage: `basename $0` <start|stop|restart|init|status> <node_id>"

if [ $# -ne 2 ]; then
	echo $usage
	exit 1
fi

curl=`which curl`

command=$1
nodeid=$2

# Load settings
root=`cd \`dirname $0\`/..;pwd`
bin=$root/bin
. $bin/gphd-env.sh

instance_root=$PXF_STORAGE_ROOT/pxf$nodeid
instance_name=pxf-service-$nodeid
instance_port=588$nodeid
jpda_port=800$nodeid

function createInstance()
{
	mkdir -p $instance_root
	mkdir -p $instance_root/$instance_name
	cp -r $TOMCAT_ROOT/* $instance_root/$instance_name/.
	
	# port change will be handled in configureInstance, default 5888.

	if [ $? -gt 0 ]; then
		echo instance creation failed
		return 1
	fi

	echo "created instance in $instance_root/$instance_name"
}

#
# configureInstance configures the tomcat instance
# 
# specifically:
#	for single cluster - update port
#
function configureInstance()
{
	catalinaProperties=$instance_root/$instance_name/conf/catalina.properties
	cat $catalinaProperties | \
	sed "s/^[[:blank:]]*connector.http.port=.*$/connector.http.port=$instance_port/g" \
	> ${catalinaProperties}.tmp

	rm $catalinaProperties
	mv ${catalinaProperties}.tmp $catalinaProperties
}

function deployWebapp()
{
	pushd $instance_root/$instance_name/lib
	ln -s $PXF_ROOT/pxf-service.jar .

	cd ../webapps
	ln -s $PXF_ROOT/pxf.war .
	popd
}

function doinit()
{
	createInstance || return 1
	configureInstance || return 1
	deployWebapp || return 1
}

function patchWebapp()
{
	pushd $instance_root/$instance_name/webapps || return 1
	rm -rf pxf
	mkdir pxf
	cd pxf
	unzip ../pxf.war
	popd

	context_file=$instance_root/$instance_name/webapps/pxf/META-INF/context.xml
	cat $context_file | \
	sed  -e "s/classpathFiles=\"[a-zA-Z0-9\/\;.-]*\"/classpathFiles=\"pxf\/conf\/pxf-private.classpath\"/" > context.xml.tmp
	mv context.xml.tmp $context_file

	web_file=$instance_root/$instance_name/webapps/pxf/WEB-INF/web.xml
	cat $web_file | \
	sed "s/<param-value>.*pxf-log4j2.xml<\/param-value>/<param-value>..\/..\/..\/..\/..\/..\/pxf\/conf\/pxf-log4j2.xml<\/param-value>/" > web.xml.tmp
	mv web.xml.tmp $web_file
}

function waitForTomcat()
{
	attempts=0
	max_attempts=$1 # try to connect given number of attempts 
	sleep_time=1 # sleep 1 second between attempts
	
	# wait until tomcat is up:
	echo Checking if tomcat is up and running...
	until [[ "`curl --silent --connect-timeout 1 -I http://localhost:${instance_port} | grep 'Coyote'`" != "" ]];
	do
		let attempts=attempts+1
		if [[ "$attempts" -eq "$max_attempts" ]]; then
			echo ERROR: PXF is down - tomcat is not running
			return 1
		fi
		echo "tomcat not responding, re-trying after ${sleep_time} second (attempt number ${attempts})"
		sleep $sleep_time
	done
	
	return 0
}

function checkWebapp()
{
	# wait for tomcat to start responding  
	waitForTomcat $1 || return 1
	
	# check if PXF webapp is up:
	echo Checking if PXF webapp is up and running...
	curlResponse=$($curl --silent "http://localhost:${instance_port}/pxf/v0")
	expectedResponse="Wrong version v0, supported version is v[0-9]+"
	
	if [[ $curlResponse =~ $expectedResponse ]]; then
		echo PXF webapp is up
		return 0
	fi
	
	echo ERROR: PXF webapp is inaccessible but tomcat is up. Check logs for more information
	return 1
}

function commandWebapp()
{
	command=$1
	$instance_root/$instance_name/bin/catalina.sh jpda $command 
	if [ $? -ne 0 ]; then
		return 1
	fi 
}

function dostart()
{
	patchWebapp || return 1
    if $PXFDEBUG ; then
        export JPDA_ADDRESS=$jpda_port
        $instance_root/$instance_name/bin/catalina.sh jpda start 
    else
        $instance_root/$instance_name/bin/catalina.sh start 
    fi
    if [ $? -ne 0 ]; then
        return 1
    fi
	checkWebapp 300 || return 1
}

function dostop()
{
    $instance_root/$instance_name/bin/catalina.sh stop
    if [ $? -ne 0 ]; then
        return 1
    fi
	#commandWebapp stop || return 1
}

function dostatus()
{
	checkWebapp 1 || return 1
}

case "$command" in
	"init" )
		doinit
		;;
	"start" )
		dostart
		;;
	"stop" )
		dostop
		;;
	"restart" )
		dostop
		dostart
		;;
	"status" )
		dostatus
		;;
	* )
		echo $usage
		exit 2
		;;
esac

exit $?
