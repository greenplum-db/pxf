#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

PARENT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

#############################################################################
# The PXF_CONF property is updated by pxf init script, do not edit manually #
#############################################################################
export PXF_CONF=${PXF_CONF:="NOT_INITIALIZED"}
#############################################################################

# load user environment first, warn if the script is missing when not in init mode
if [[ $pxf_script_command != init && $pxf_cluster_command != init ]]; then
	if [[ ${PXF_CONF} == "NOT_INITIALIZED" ]]; then
		echo "ERROR: PXF is not initialized, call pxf init command"
		exit 1
	fi
	user_env_script=${PXF_CONF}/conf/pxf-env.sh
	if [[ ! -f ${user_env_script} ]]; then
	    echo "WARNING: failed to find ${user_env_script}, default parameters will be used"
	else
		source ${user_env_script}
	fi
fi

# Default PXF_HOME
export PXF_HOME=${PXF_HOME:=${PARENT_SCRIPT_DIR}}

# Path to HDFS native libraries
export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native:${LD_LIBRARY_PATH}

# Path to JAVA
export JAVA_HOME=${JAVA_HOME:=/usr/java/default}

# Path to Log directory
export PXF_LOGDIR=${PXF_LOGDIR:=${PXF_CONF}/logs}

# Path to Run directory
export PXF_RUNDIR=${PXF_HOME}/run

# Port
export PXF_PORT=${PXF_PORT:=5888}

# Shutdown Port
export PXF_SHUTDOWN_PORT=${PXF_SHUTDOWN_PORT:=5889}

# Memory
export PXF_JVM_OPTS=${PXF_JVM_OPTS:="-Xmx2g -Xms1g"}

# Threads
export PXF_MAX_THREADS=${PXF_MAX_THREADS:=200}

# Kerberos path to keytab file owned by pxf service with permissions 0400
export PXF_KEYTAB=${PXF_KEYTAB:="${PXF_CONF}/keytabs/pxf.service.keytab"}

# Kerberos principal pxf service should use. _HOST is replaced automatically with hostnames FQDN
export PXF_PRINCIPAL=${PXF_PRINCIPAL:="gpadmin/_HOST@EXAMPLE.COM"}

# End-user identity impersonation, set to true to enable
export PXF_USER_IMPERSONATION=${PXF_USER_IMPERSONATION:=true}

# Set to true to enable Remote debug via port 8000
export PXF_DEBUG=${PXF_DEBUG:-false}

# Fragmenter cache, set to false to disable
export PXF_FRAGMENTER_CACHE=${PXF_FRAGMENTER_CACHE:-true}

# Kill PXF on OutOfMemoryError, set to false to disable
export PXF_OOM_KILL=${PXF_OOM_KILL:-true}

# Dump heap on OutOfMemoryError, set to dump path to enable
# export PXF_OOM_DUMP_PATH=
