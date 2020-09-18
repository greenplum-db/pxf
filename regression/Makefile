ifndef GPHOME
$(error GPHOME is not defined)
endif
ifndef PXF_HOME
$(error PXF_HOME is not defined)
endif

PXF_BASE ?= $(PXF_HOME)

REGRESS_OPTS = --init-file=./init_file

SUBSTITUTE_COMMAND = scripts/substitute.bash

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifdef PXF_TEST_DEBUG
SUBSTITUTE_COMMAND += --debug
REGRESS_OPTS += --debug
endif

# we get this variable for free on 6X, but not on 5X
pg_regress_installcheck=$(shell find "$$GPHOME" -name pg_regress)

setup_test_dir :
	mkdir -p /tmp/pxf_automation_data

setup_script_dir :
	mkdir -p scripts/generated

setup_dirs: setup_test_dir setup_script_dir

# 5X (Postgres 8) has no --load-extension flag (see targets below)
# substitute.bash handles extension loading and different 5X/6X COPY syntax
fdw_%_schedule : setup_dirs
	@if $(PG_CONFIG) | grep -q 'VERSION.*PostgreSQL 8'; then \
		GPDB_5X_STABLE=1 $(SUBSTITUTE_COMMAND) schedules/$@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=schedules/$@; \
	else \
		$(SUBSTITUTE_COMMAND) schedules/$@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) --load-extension=pxf_fdw --schedule=schedules/$@; \
	fi

%_schedule : setup_dirs
	@if $(PG_CONFIG) | grep -q 'VERSION.*PostgreSQL 8'; then \
		GPDB_5X_STABLE=1 $(SUBSTITUTE_COMMAND) schedules/$@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=schedules/$@; \
	else \
		$(SUBSTITUTE_COMMAND) schedules/$@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) --load-extension=pxf --schedule=schedules/$@; \
	fi

FDW_%Test : setup_dirs
	@if $(PG_CONFIG) | grep -q 'VERSION.*PostgreSQL 8'; then \
		GPDB_5X_STABLE=1 $(SUBSTITUTE_COMMAND) $@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) _$@; \
	else \
		$(SUBSTITUTE_COMMAND) $@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) --load-extension=pxf_fdw _$@; \
	fi

%Test : setup_dirs
	@if $(PG_CONFIG) | grep -q 'VERSION.*PostgreSQL 8'; then \
		GPDB_5X_STABLE=1 $(SUBSTITUTE_COMMAND) $@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) _$@; \
	else \
		$(SUBSTITUTE_COMMAND) $@; \
		$(pg_regress_installcheck) $(REGRESS_OPTS) --load-extension=pxf _$@; \
	fi

clean :
	rm -rf /tmp/pxf_automation_data
	rm -rf sql/_*.sql expected/_*.out results/*
	if ls scripts/generated/*.bash; then \
		for script in scripts/generated/*bash; do \
			"$${script}"; \
		done; \
	fi
	if ls scripts/generated/*cleanup*.sql; then \
		for script in scripts/generated/*cleanup*sql; do \
			$(BEELINE_CMD) -f "$${script}"; \
		done; \
	fi
	if ls scripts/generated/*drop*.rb; then \
		for script in scripts/generated/*drop*.rb; do \
			$(HBASE_CMD) shell "$${PWD}/$${script}"; \
		done; \
	fi
	rm -rf scripts/generated/*
