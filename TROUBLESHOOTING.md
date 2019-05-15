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

## S3

### Issues with credentials

You may see an error originating at listObjects when AWS credentials are not synced correctly across the PXF cluster.
You may see the following error message:


    ERROR: remote component error (500) from '127.0.0.1:5888':  type  Exception report   message   javax.servlet.ServletException: org.apache.hadoop.fs.s3a.AWSClientIOException: listObjects() on s3a://bucket/path:
    com.amazonaws.SdkClientException: Failed to sanitize XML document destined for handler class com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser$ListBucketHandler: Failed to sanitize XML document destined for handler class com.amazonaws.services.s3.model.transform.XmlResponsesSaxParser$ListBucketHandler
    description   The server encountered an internal error that prevented it from fulfilling this request.

To resolve this issue, you will need to sync your pxf cluster and try again the query.

    gpadmin@mdw$ pxf cluster sync

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
