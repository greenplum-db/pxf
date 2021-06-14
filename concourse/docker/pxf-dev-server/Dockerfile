ARG PROJECT_ID
ARG BASE_IMAGE=gcr.io/${PROJECT_ID}/gpdb-pxf-dev/gpdb6-centos7-test-pxf:latest

FROM ${BASE_IMAGE} as scratch

ADD singlecluster-*.tar.gz /

ADD pxf-build-dependencies.tar.gz /tmp/build-dependencies

ADD pxf-automation-dependencies.tar.gz /tmp/automation-dependencies

RUN cd / && mv singlecluster-* singlecluster && chmod a+w singlecluster

FROM ${BASE_IMAGE}

COPY --from=scratch /singlecluster /singlecluster

COPY --from=scratch /tmp/build-dependencies/ /home/gpadmin/

COPY --from=scratch /tmp/automation-dependencies/ /home/gpadmin/

RUN mkdir -p /etc/hadoop/conf /etc/hive/conf /etc/hbase/conf && \
    ln -s /singlecluster/hadoop/etc/hadoop/*-site.xml /etc/hadoop/conf && \
    ln -s /singlecluster/hive/conf/hive-site.xml /etc/hive/conf && \
    ln -s /singlecluster/hbase/conf/hbase-site.xml /etc/hbase/conf && \
    ln -s ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} ~root && \
    chown -R gpadmin:gpadmin ~gpadmin/.{tomcat,go-dep-cached-sources,m2,gradle} && \
    chmod a+w /singlecluster