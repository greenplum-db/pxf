# Troubleshooting

## Out of Memory Issues

### 

When debugging `OutOfMemoryError`, you can set the environment variable `$PXF_OOM_DUMP_PATH` in `${PXF_CONF}/conf/pxf-env.sh`.
This results in these flags being added during JVM startup:

```
-XX:+HeapDumpOnOutOfMemoryError -XX:+HeapDumpPath=$PXF_OOM_DUMP_PATH
```

Heap dump files are usually large (GBs), so make sure you have enough disk space at `$PXF_OOM_DUMP_PATH` in case of an `OutOfMemoryError`.

If `$PXF_OOM_DUMP_PATH` is a directory, a new java heap dump file will be generated at `$PXF_OOM_DUMP_PATH/java_pid<PID>.hprof` for each `OutOfMemoryError`, where `<PID>` is the process ID of the PXF server instance.
If `$PXF_OOM_DUMP_PATH` is not a directory, a single Java heap dump file will be generated on the first `OutOfMemoryError`, but will not be overwritten on subsequent `OutOfMemoryError`s, so rename files accordingly.

### Generate a heap dump for PXF

    jmap -dump:live,format=b,file=<your_location>/heap_dump "$(pgrep -f tomcat)"

* Note: `live` will force a full garbage collection before dump

### Collect JVM Statistics from PXF

The following command will collect Java Virtual Machine Statistics every 60
seconds.

     jstat -gcutil $(pgrep -f tomcat) 60000 > /tmp/jstat_pxf_1min.out &

## Dataproc

### Accessing Dataproc clusters from external network

Dataproc uses the internal IP addresses for the partition locations. We can ask
dataproc to use the datanode hostnames instead. We need to set a property in hdfs-site.xml 
`dfs.client.use.datanode.hostname`=true.

- `dfs.client.use.datanode.hostname`: By default HDFS
   clients connect to DataNodes using the IP address
   provided by the NameNode. Depending on the network
   configuration this IP address may be unreachable by
   the clients. The fix is letting clients perform
   their own DNS resolution of the DataNode hostname.
   The following setting enables this behavior.

      <property>
          <name>dfs.client.use.datanode.hostname</name>
          <value>true</value>
          <description>Whether clients should use datanode hostnames when
              connecting to datanodes.
          </description>
      </property>

### Dataproc with Kerberos

A kerberized dataproc cluster might start with permission checking turned off.
This means that any user will be able to access any file in the HDFS cluster.
To enable permission checking modify hdfs-site.xml

      <property>
          <name>dfs.permissions.enabled</name>
          <value>true</value>
      </property>