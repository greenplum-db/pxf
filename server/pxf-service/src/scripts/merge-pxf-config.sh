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

# Source the pxf-env.sh file
. "${PXF_CONF}/conf/pxf-env.sh"

header_added_to_properties=false
header_added_to_env=false

function addHeader()
{
  echo "

# ******************************************************
# * The properties below were added by the pxf migrate
# * tool on $(date)
# ******************************************************
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

# now let's take a look at all the variables loaded from the file
for var in $(compgen -e); do
  if [[ $var == PWD ]] || [[ $var == SHLVL ]] || [[ $var == _ ]]; then
    # These are environment variables loaded by default in the shell - skip them
    continue
  elif [[ $var == PXF_BASE ]] || [[ $var == PXF_CONF ]]; then
    # PXF_BASE and PXF_CONF are used to run this script - skip them
    continue
  elif [[ $var == PXF_KEYTAB ]] || [[ $var == PXF_PRINCIPAL ]] || [[ $var == PXF_USER_IMPERSONATION ]]; then
    # We don't migrate deprecated properties because the configuration has changed
    echoYellow "The $var property has been deprecated and it won't be automatically migrated. Please perform a manual migration for $var"
  elif [[ $var == PXF_MAX_THREADS ]]; then
    echoGreen " - Migrating PXF_MAX_THREADS=$PXF_MAX_THREADS to '${PXF_BASE}/conf/pxf-application.properties' as \"pxf.max.threads\""
    ensureHeaderAddedToProperties
    echo "pxf.max.threads=$PXF_MAX_THREADS" >> "${PXF_BASE}/conf/pxf-application.properties"
  elif [[ $var == PXF_FRAGMENTER_CACHE ]]; then
    echoGreen " - Migrating PXF_FRAGMENTER_CACHE=$PXF_FRAGMENTER_CACHE to '${PXF_BASE}/conf/pxf-application.properties' as \"pxf.metadata-cache-enabled\""
    ensureHeaderAddedToProperties
    echo "pxf.metadata-cache-enabled=$PXF_FRAGMENTER_CACHE" >> "${PXF_BASE}/conf/pxf-application.properties"
  else
    echoGreen " - Migrating $var=${!var} to "${PXF_BASE}/conf/pxf-env.sh""
    # JAVA_HOME, PXF_LOGDIR, PXF_JVM_OPTS, PXF_OOM_KILL, PXF_OOM_DUMP_PATH, LD_LIBRARY_PATH and others
    ensureHeaderAddedToEnv
    echo "export $var=\"${!var}\"" >> "${PXF_BASE}/conf/pxf-env.sh"
  fi
done
