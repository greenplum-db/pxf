ARG BASE_IMAGE=pivotaldata/gpdb-pxf-dev:centos6

FROM ${BASE_IMAGE} as scratch

ADD singlecluster-*.tar.gz /

RUN cd / && mv singlecluster-* singlecluster && chmod a+w singlecluster

ADD pxf-build-dependencies.tar.gz /tmp/build-dependencies

FROM ${BASE_IMAGE}

COPY --from=scratch /singlecluster /singlecluster

COPY --from=scratch /tmp/build-dependencies/ /home/gpadmin/

RUN mkdir -p /etc/hadoop/conf /etc/hive/conf /etc/hbase/conf && \
    ln -s /singlecluster/hadoop/etc/hadoop/core-site.xml /etc/hadoop/conf && \
    ln -s /singlecluster/hadoop/etc/hadoop/hdfs-site.xml /etc/hadoop/conf && \
    ln -s /singlecluster/hadoop/etc/hadoop/mapred-site.xml /etc/hadoop/conf && \
    ln -s /singlecluster/hadoop/etc/hadoop/yarn-site.xml /etc/hadoop/conf && \
    ln -s /singlecluster/hive/conf/hive-site.xml /etc/hive/conf && \
    ln -s /singlecluster/hbase/conf/hbase-site.xml /etc/hbase/conf && \
    ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root && \
    chown -R gpadmin:gpadmin ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle}
