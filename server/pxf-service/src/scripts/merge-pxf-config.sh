#!/bin/bash

# This script runs on a bash with minimal environment variables loaded, for
# example:
# env -i "$BASH" -c "PXF_HOME=$PXF_HOME PXF_BASE=$PXF_BASE PXF_CONF=$PXF_CONF $PXF_HOME/bin/merge-pxf-config.sh"
# The script will look at the environment variables loaded from
# ${PXF_CONF}/conf/pxf-env.sh and merges those configurations into
# ${PXF_BASE}/conf/pxf-env.sh, ${PXF_BASE}/conf/pxf-application.properties,
# and ${PXF_BASE}/servers/default/pxf-site.xml

: "${PXF_CONF:?PXF_CONF must be set}"
: "${PXF_BASE:?PXF_BASE must be set}"
: "${PXF_HOME:?PXF_HOME must be set}"

# ANSI Colors
echoRed() { echo $'\e[0;31m'"$1"$'\e[0m'; }
echoGreen() { echo $'\e[0;32m'"$1"$'\e[0m'; }
echoYellow() { echo $'\e[0;33m'"$1"$'\e[0m'; }

# print error message and return with error code
function fail() {
    echoRed "ERROR: $1"
    exit 1
}

# FILE1 -ef FILE2  True if file1 is a hard link to file2.
[[ ! "$PXF_CONF" -ef "$PXF_BASE" ]] || fail "The PXF_CONF directory must be different from your target directory '$PXF_BASE'"

# Source the pxf-env.sh file
. "${PXF_CONF}/conf/pxf-env.sh"

header_added_to_properties=false
header_added_to_env=false

function addHeader() {
    echo "

######################################################
# The properties below were added by the pxf migrate
# tool on $(date)
######################################################
" >> "$1"
}

function ensureHeaderAddedToProperties() {
    if [[ $header_added_to_properties == false ]]; then
        header_added_to_properties=true
        addHeader "${PXF_BASE}/conf/pxf-application.properties"
    fi
}

function ensureHeaderAddedToEnv() {
    if [[ $header_added_to_env == false ]]; then
        header_added_to_env=true
        addHeader "${PXF_BASE}/conf/pxf-env.sh"
    fi
}

# Add the value to the pxf-env.sh file
function addToEnv() {
    local var=$1
    echoGreen " - Migrating $var=${!var} to "${PXF_BASE}/conf/pxf-env.sh""
    ensureHeaderAddedToEnv
    echo "export $var=\"${!var}\"" >> "${PXF_BASE}/conf/pxf-env.sh"
}

# Add the value to the pxf-application.properties file
function addToProperties() {
    local var=$1
    local newName=$2
    echoGreen " - Migrating $var=${!var} to '${PXF_BASE}/conf/pxf-application.properties' as \"$newName\""
    ensureHeaderAddedToProperties
    echo "$newName=${!var}" >> "${PXF_BASE}/conf/pxf-application.properties"
}

# If the default/pxf-site.xml file does not exist copy it from template
# then add the value to pxf-site.xml if the existing value is different from
# the default value
function addToDefaultPxfSite() {
    local var=$1
    local propertyName=$2
    local defaultValue=$3

    if [[ ! -f ${PXF_BASE}/servers/default/pxf-site.xml ]]; then
        echoGreen " - Creating pxf-site.xml in ${PXF_BASE}/servers/default/"
        cp "${PXF_HOME}/templates/pxf-site.xml" "${PXF_BASE}/servers/default/pxf-site.xml"
    fi

    existingValue=$(sed -ne "/<name>${propertyName}<\/name>/{n;s/.*<value>\(.*\)<\/value>.*/\1/p;q;}" "${PXF_BASE}/servers/default/pxf-site.xml")
    if [[ "${existingValue}" == "${defaultValue}" ]]; then
        echoGreen " - Migrating $var=${!var} to '${PXF_BASE}/servers/default/pxf-site.xml' as \"$propertyName\""
        sed -i "/<name>${propertyName}<\/name>/ {n;s|<value>.*</value>|<value>${!var}</value>|g;}" "${PXF_BASE}/servers/default/pxf-site.xml"
    else
        echoYellow " - Not migrating $var=${!var} because the existing value (${existingValue}) is not the default value (${defaultValue}) in ${PXF_BASE}/servers/default/pxf-site.xml"
    fi
}

function warnRemovedProperties() {
    local var=$1
    # We don't migrate removed properties because the configuration has changed
    echoYellow "The $var property has been removed and it won't be automatically migrated."
    echoYellow "Please refer to documentation to perform manual migration for impersonation and Kerberos principals"
}

# Properties migrated to pxf-env.sh
[[ -n $JAVA_HOME ]] && addToEnv "JAVA_HOME"
[[ -n $PXF_LOGDIR ]] && addToEnv "PXF_LOGDIR"
[[ -n $PXF_JVM_OPTS ]] && addToEnv "PXF_JVM_OPTS"
[[ -n $PXF_OOM_KILL ]] && addToEnv "PXF_OOM_KILL"
[[ -n $PXF_OOM_DUMP_PATH ]] && addToEnv "PXF_OOM_DUMP_PATH"
[[ -n $LD_LIBRARY_PATH ]] && addToEnv "LD_LIBRARY_PATH"
[[ -n $PXF_RUNDIR ]] && addToEnv "PXF_RUNDIR"
[[ -n $PXF_PORT ]] && addToEnv "PXF_PORT"

# Properties migrated to pxf-application.properties
[[ -n $PXF_MAX_THREADS ]] && addToProperties "PXF_MAX_THREADS" "pxf.max.threads"

# Properties migrated to pxf-site.xml of the default server
[[ -n $PXF_KEYTAB ]] && addToDefaultPxfSite 'PXF_KEYTAB' 'pxf.service.kerberos.keytab' '${pxf.conf}/keytabs/pxf.service.keytab'
[[ -n $PXF_PRINCIPAL ]] && addToDefaultPxfSite 'PXF_PRINCIPAL' 'pxf.service.kerberos.principal' 'gpadmin/_HOST@EXAMPLE.COM'

# Removed properties
[[ -n $PXF_USER_IMPERSONATION ]] && warnRemovedProperties "PXF_USER_IMPERSONATION"
