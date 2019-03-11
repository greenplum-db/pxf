# Troubleshooting

## Out of Memory Issues

### 

-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<desired_path_for_heap_dump>


### Generate a heapdump for PXF

    jmap -dump:live,format=b,file=<your_location>/heap_dump "$(pgrep -f tomcat)"

* Note: `live` will force a full garbage collection before dump


## Accessing Dataproc clusters from external network

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
