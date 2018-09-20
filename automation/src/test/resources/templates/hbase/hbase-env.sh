# Set environment variables here.

# The java implementation to use.  Java 1.6 required.
export JAVA_HOME=/usr/java/default

# The maximum amount of heap to use, in MB. Default is 1000.
export HBASE_HEAPSIZE=2048

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export HBASE_OPTS="-XX:+UseConcMarkSweepGC"

# Uncomment below to enable java garbage collection logging in the .out file.
export HBASE_OPTS="-ea -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode"

# Uncomment below if you intend to use the EXPERIMENTAL off heap cache.
# export HBASE_OPTS="$HBASE_OPTS -XX:MaxDirectMemorySize="
# Set hbase.offheapcache.percentage in hbase-site.xml to a nonzero value.


# Uncomment and adjust to enable JMX exporting
# See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
# More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
#
# export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
# export HBASE_MASTER_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10101"
# export HBASE_REGIONSERVER_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10102"
# export HBASE_THRIFT_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10103"
# export HBASE_ZOOKEEPER_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10104"

# Set HADOOP_HOME to point to a specific hadoop install directory
export HADOOP_HOME="/usr/lib/gphd/hadoop"
export HADOOP_CONF_DIR="/etc/gphd/hadoop/conf"

# Hbase Configuration Directory can be controlled by:
export HBASE_CONF_DIR="/etc/gphd/hbase/conf"

# Extra Java CLASSPATH elements.  Optional.
export HBASE_CLASSPATH=${HBASE_CLASSPATH}:${HADOOP_CONF_DIR}:/usr/lib/gphd/pxf/pxf-hbase-*[0-9].jar

# File naming hosts on which HRegionServers will run.  $HBASE_HOME/conf/regionservers by default.
export HBASE_REGIONSERVERS=${HBASE_CONF_DIR}/regionservers

# Extra ssh options.  Empty by default.
# export HBASE_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=HBASE_CONF_DIR"

# Where log files are stored.  $HBASE_HOME/logs by default.
#export HBASE_LOG_DIR="/var/log/gphd/hbase"

# A string representing this instance of hbase. $USER by default.
# export HBASE_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HBASE_NICENESS=10

# The directory where pid files are stored. /tmp by default.
#export HBASE_PID_DIR=/var/run/gphd/hbase

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HBASE_SLAVE_SLEEP=0.1

# Tell HBase whether it should manage it's own instance of Zookeeper or not.
export HBASE_MANAGES_ZK=false

# GPHD variables
export GPHD_ROOT=/usr/lib/gphd

# Required for GPXF with HBase
export HBASE_CLASSPATH=${HBASE_CLASSPATH}:\
$GPHD_ROOT/pxf/pxf-hbase.jar

# Required for running mapreduce jobs
export HBASE_CLASSPATH=${HBASE_CLASSPATH}:\
$GPHD_ROOT/hadoop-yarn/*:\
$GPHD_ROOT/hadoop-mapreduce/*




