EXTENSION   = pxf
DATA        = pxf--2.0.sql pxf--1.0--2.0.sql pxf--1.0.sql
MODULE_big  = pxf
OBJS        = src/pxfprotocol.o src/pxfbridge.o src/pxfuriparser.o src/libchurl.o src/pxfutils.o src/pxfheaders.o src/gpdbwritableformatter.o src/pxffilters.o
REGRESS     = setup pxf pxfinvalid
SHLIB_LINK += -lcurl

PXF_API_VERSION := $(shell cat ../api_version)
PG_CPPFLAGS     := -DPXF_API_VERSION=\"$(PXF_API_VERSION)\"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
ifndef PGXS
    $(error Make sure the Greenplum installation binaries are in your PATH. i.e. export PATH=<path to your Greenplum installation>/bin:$$PATH)
endif
include $(PGXS)

.PHONY: stage
stage: pxf.so
	mkdir -p build/stage
	install -c -m 755 pxf.so build/stage/pxf.so
	install -c -m 644 pxf.control build/stage/
	install -c -m 644 pxf--1.0.sql build/stage/
	install -c -m 644 pxf--2.0.sql build/stage/
	install -c -m 644 pxf--1.0--2.0.sql build/stage/
	@echo "gpdb.version=$(GP_VERSION)" > build/stage/metadata
	@echo "gpdb.major-version=$(GP_MAJORVERSION)" >> build/stage/metadata

.PHONY: clean-all
clean-all: clean
	rm -rf build
