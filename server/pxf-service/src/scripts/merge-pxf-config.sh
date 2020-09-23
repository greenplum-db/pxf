#!/bin/bash

# This script runs on a bash with minimal environment variables loaded, for
# example:
# env -i "$BASH" -c "PXF_BASE=$PXF_BASE PXF_CONF=$PXF_CONF $PXF_HOME/bin/merge-pxf-config.sh"
# The script will look at the environment variables loaded from
# ${PXF_CONF}/conf/pxf-env.sh and merges those configurations into
# "${PXF_BASE}/conf/pxf-env.sh" and "${PXF_BASE}/conf/pxf-application.properties"

: "${PXF_CONF:?PXF_CONF must be set}"
: "${PXF_BASE:?PXF_BASE must be set}"

# ANSI Colors
echoRed() { echo $'\e[0;31m'"$1"$'\e[0m'; }
echoGreen() { echo $'\e[0;32m'"$1"$'\e[0m'; }
echoYellow() { echo $'\e[0;33m'"$1"$'\e[0m'; }

# print error message and return with error code
function fail()
{
  echoRed "ERROR: $1"
  exit 1
}

# FILE1 -ef FILE2  True if file1 is a hard link to file2.
[[ ! "$PXF_CONF" -ef "$PXF_BASE" ]] || fail "The PXF_CONF directory must be different from your target directory '$PXF_BASE'"

# Source the pxf-env.sh file
. "${PXF_CONF}/conf/pxf-env.sh"

header_added_to_properties=false
header_added_to_env=false

function addHeader()
{
  echo "

######################################################
# The properties below were added by the pxf migrate
# tool on $(date)
######################################################
" >> "$1"
}

function ensureHeaderAddedToProperties()
{
  if [[ $header_added_to_properties == false ]]; then
    header_added_to_properties=true
    addHeader "${PXF_BASE}/conf/pxf-application.properties"
  fi
}

function ensureHeaderAddedToEnv()
{
  if [[ $header_added_to_env == false ]]; then
    header_added_to_env=true
    addHeader "${PXF_BASE}/conf/pxf-env.sh"
  fi
}

# Add the value to the pxf-env.sh file
function addToEnv()
{
  local var=$1
  echoGreen " - Migrating $var=${!var} to "${PXF_BASE}/conf/pxf-env.sh""
  ensureHeaderAddedToEnv
  echo "export $var=\"${!var}\"" >> "${PXF_BASE}/conf/pxf-env.sh"
}

# Add the value to the pxf-application.properties file
function addToProperties()
{
  local var=$1
  local newName=$2
  echoGreen " - Migrating $var=${!var} to '${PXF_BASE}/conf/pxf-application.properties' as \"$newName\""
  ensureHeaderAddedToProperties
  echo "$newName=${!var}" >> "${PXF_BASE}/conf/pxf-application.properties"
}

function warnRemovedProperties()
{
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
[[ -n $PXF_FRAGMENTER_CACHE ]] && addToProperties "PXF_FRAGMENTER_CACHE" "pxf.metadata-cache-enabled"

# Removed properties
[[ -n $PXF_KEYTAB ]] && warnRemovedProperties "PXF_KEYTAB"
[[ -n $PXF_PRINCIPAL ]] && warnRemovedProperties "PXF_PRINCIPAL"
[[ -n $PXF_USER_IMPERSONATION ]] && warnRemovedProperties "PXF_USER_IMPERSONATION"