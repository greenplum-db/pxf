ARG BASE_IMAGE

FROM pivotaldata/${BASE_IMAGE} as scratch

RUN cd pxf_src/server && make tar

RUN PWD=$(pwd) && cd pxf_src/automation && PXF_HOME=${PWD}/pxf_src/server/build/stage make dev

FROM pivotaldata/${BASE_IMAGE}

RUN mkdir -p /home/gpadmin

COPY --from=scratch /home/gpadmin/.m2 /root/.m2

COPY --from=scratch /root/.gradle /root/.gradle
