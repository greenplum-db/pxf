# Troubleshooting

## Out of Memory Issues

### 

-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<desired_path_for_heap_dump>


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
