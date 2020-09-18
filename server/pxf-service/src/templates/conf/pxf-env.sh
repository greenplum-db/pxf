#!/bin/bash

##############################################################################
# This file contains PXF properties that can be specified by users           #
# to customize their deployments. This file is sourced by PXF Server control #
# scripts upon initialization, start and stop of the PXF Server.             #
#                                                                            #
# To update a property, uncomment the line and provide a new value.          #
##############################################################################

# Path to JAVA
# export JAVA_HOME=/usr/java/default

# Path to Log directory
# export PXF_LOGDIR="${PXF_BASE}/logs"

# Memory
# export PXF_JVM_OPTS="-Xmx2g -Xms1g"

# Kill PXF on OutOfMemoryError, set to false to disable
# export PXF_OOM_KILL=true

# Dump heap on OutOfMemoryError, set to dump path to enable
# export PXF_OOM_DUMP_PATH=${PXF_BASE}/run/pxf_heap_dump

# Additional locations to be class-loaded by PXF
# export PXF_LOADER_PATH=

# Additional native libraries to be loaded by PXF
# export LD_LIBRARY_PATH=
