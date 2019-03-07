# Troubleshooting

## Out of Memory Issues

### 

-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<desired_path_for_heap_dump>


### Generate a heapdump for PXF

    jmap -dump:live,format=b,file=<your_location>/heap_dump "$(pgrep -f tomcat)"

* Note: `live` will force a full garbage collection before dump