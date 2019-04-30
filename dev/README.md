# Profiling

### Visual VM Profiling

To perform memory profiling add the following line to PXF's environment settings (`pxf/conf/pxf-env.sh`) on the machine where we want to debug:

```
export CATALINA_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote.port=9090 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=127.0.0.1"
```

### JProfiler

To perform memory profiling in JProfiler add the following setting to your `PXF_JVM_OPTS`:

```
export PXF_JVM_OPTS="-Xmx2g -Xms1g -agentpath:/Applications/JProfiler.app/Contents/Resources/app/bin/macos/libjprofilerti.jnilib=port=8849"
```
