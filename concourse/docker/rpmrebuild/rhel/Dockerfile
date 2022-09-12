ARG BASE_IMAGE=gcr.io/data-gpdb-private-images/gpdb6-rhel8-test:latest

FROM centos:7 as downloader

# download source RPM for rpmrebuild from EPEL repository for EL7
RUN yum install -y epel-release && yumdownloader --source rpmrebuild

FROM ${BASE_IMAGE}

COPY --from=downloader /rpmrebuild-*.src.rpm /

# build EL8 RPM for rpmrebuild from the downloaded source RPM
RUN rpmbuild --rebuild rpmrebuild-*.src.rpm \
    && rpm -i /root/rpmbuild/RPMS/noarch/rpmrebuild-*.el8.noarch.rpm \
    && rm rpmrebuild-*.src.rpm \
    && rm -rf /root/rpmbuild

RUN yum install libcgroup-tools
