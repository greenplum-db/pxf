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
export PXF_CONF=${PXF_CONF:-NOT_INITIALIZED}
#############################################################################

[[ -f ${PXF_CONF}/conf/pxf-env.sh ]] && source "${PXF_CONF}/conf/pxf-env.sh"

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

# Memory
export PXF_JVM_OPTS=${PXF_JVM_OPTS:='-Xmx2g -Xms1g'}

# Threads
export PXF_MAX_THREADS=${PXF_MAX_THREADS:=200}

# Set to true to enable Remote debug via port 2020
export PXF_DEBUG=${PXF_DEBUG:-false}

# Fragmenter cache, set to false to disable
export PXF_FRAGMENTER_CACHE=${PXF_FRAGMENTER_CACHE:-true}

# Kill PXF on OutOfMemoryError, set to false to disable
export PXF_OOM_KILL=${PXF_OOM_KILL:-true}

# Dump heap on OutOfMemoryError, set to dump path to enable
# export PXF_OOM_DUMP_PATH=

# Additional locations to be class-loaded by the application
if [[ -n "${PXF_LOADER_PATH}" ]]; then
  export LOADER_PATH="${PXF_LOADER_PATH},file:${PXF_CONF}/conf,file:${PXF_HOME}/conf,file:${PXF_CONF}/lib"
else
  export LOADER_PATH="file:${PXF_CONF}/conf,file:${PXF_HOME}/conf,file:${PXF_CONF}/lib"
fi
