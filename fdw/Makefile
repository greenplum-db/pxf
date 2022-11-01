# fdw/Makefile

MODULE_big = pxf_fdw
OBJS = pxf_fdw.o pxf_bridge.o pxf_deparse.o pxf_filter.o pxf_header.o pxf_option.o libchurl.o

SHLIB_LINK += -lcurl

EXTENSION = pxf_fdw
DATA = pxf_fdw--1.0.sql

REGRESS = pxf_fdw_wrapper pxf_fdw_server pxf_fdw_user_mapping pxf_fdw_foreign_table

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PXF_API_VERSION := $(shell cat ../api_version)
PG_CPPFLAGS := -DPXF_API_VERSION=\"$(PXF_API_VERSION)\"

ifndef PGXS
    $(error Make sure the Greenplum installation binaries are in your PATH. i.e. export PATH=<path to your greenplum installation>/bin:$$PATH)
endif

include $(PGXS)

.PHONY: stage
stage: pxf_fdw.so
	mkdir -p build/metadata
	mkdir -p build/stage
	install -c -m 755 pxf_fdw.so build/stage/pxf_fdw.so
	install -c -m 644 pxf_fdw.control build/stage/
	install -c -m 644 pxf_fdw--1.0.sql build/stage/
	@echo $(GP_MAJORVERSION) > build/metadata/gp_major_version
	@echo $(BLD_ARCH) | sed 's/_/-/' > build/metadata/build_arch

.PHONY: clean-all
clean-all: clean
	rm -rf build

test: install installcheck