#!/bin/bash

##############################################################################
# This file contains PXF properties that can be specified by users           #
# to customize their deployments. This file is sourced by PXF Server control #
# scripts upon initialization, start and stop of the PXF Server.             #
#                                                                            #
# To update a property, uncomment the line and provide a new value.          #
##############################################################################

PXF_CONF="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# Path to JAVA
# export JAVA_HOME=/usr/java/default

# Path to Log directory
# export PXF_LOGDIR="${PXF_CONF}/logs"

# Memory
# export PXF_JVM_OPTS="-Xmx2g -Xms1g"

# Threads
# export PXF_MAX_THREADS="200"

# Fragmenter cache, set to false to disable
# export PXF_FRAGMENTER_CACHE=true

# Kill PXF on OutOfMemoryError, set to false to disable
# export PXF_OOM_KILL=true

# Dump heap on OutOfMemoryError, set to dump path to enable
# export PXF_OOM_DUMP_PATH=/tmp/pxf_heap_dump
