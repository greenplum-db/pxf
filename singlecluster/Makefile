ROOT = .

BUILDVER=$(shell cat product.version)

BUILDNUM = $(BUILD_NUMBER)
ifeq ($(BUILDNUM),)
  BUILDNUM = $(shell whoami)
endif

HADOOP_VERSION = undefined
HADOOP_DISTRO = HDP

TARGET = singlecluster-$(HADOOP_DISTRO).tar.gz

BUILDROOT = $(TARGET:%.tar.gz=%)
BINROOT = $(ROOT)/bin
TARSROOT = $(ROOT)/tars
TEMPLATESROOT = $(ROOT)/templates
DOCSROOT = $(ROOT)/docs

VERSIONSFILE = $(BUILDROOT)/versions.txt

BINFILES = $(filter-out *~, $(wildcard $(BINROOT)/*))
TARFILES = $(subst $(TARSROOT)/,,$(wildcard $(TARSROOT)/*.tar.gz))
EXTRACTEDTARS = $(TARFILES:%.tar.gz=%.extracted)
TEMPLATES := $(shell find $(TEMPLATESROOT) -type f -not -iname "*~")
STACK = $(shell echo $(HADOOP_DISTRO) | tr A-Z a-z)

ALLTARGETS = singlecluster-*
DIRT = *.extracted *~

# Do not run this build script in parallel
.NOTPARALLEL:

.PHONY: all
all: clean $(TARGET)

.PHONY: clean
clean:
	-rm -rf $(ALLTARGETS)
	-rm -rf $(DIRT)

$(TARGET): $(BUILDROOT) make_tarball
# $(TARGET): $(BUILDROOT)

$(BUILDROOT): copy_binfiles create_versions_file extract_products \
	 	 	 copy_templates copy_deps copy_docs
	chmod -R +w $(BUILDROOT)

.PHONY: copy_binfiles
copy_binfiles: $(BINFILES)
	mkdir -p $(BUILDROOT)/bin
	cp $^ $(BUILDROOT)/bin

.PHONY: create_versions_file
create_versions_file:
	echo build number: $(BUILDNUM) > $(VERSIONSFILE)
	echo single_cluster-$(BUILDVER) >> $(VERSIONSFILE)

.PHONY: extract_products
extract_products: $(EXTRACTEDTARS) extract_stack_$(STACK) rename_tomcat
	for X in $(BUILDROOT)/*-[0-9]*; do \
		mv $$X `echo $$X | sed -e 's/^\($(BUILDROOT)\/[A-Za-z0-9]*\).*$$/\1/'`; \
	done;
	chmod -R +w $(BUILDROOT)
	du -sh $(BUILDROOT)/*

.PHONY: extract_stack_cdh
extract_stack_cdh:
	find $(BUILDROOT)/$(HADOOP_DISTRO)-$(HADOOP_VERSION) -iwholename "*.tar.gz" | \
		grep "\(hadoop\|zookeeper\|hive\|hbase\)" | \
		xargs -n1 tar -C $(BUILDROOT) -xzf
	rm -rf $(BUILDROOT)/hbase*/docs $(BUILDROOT)/hadoop*/src $(BUILDROOT)/hadoop*/share/doc
	rm -rf $(BUILDROOT)/$(HADOOP_DISTRO)-$(HADOOP_VERSION)
	chown root:root -R $(BUILDROOT)/* || true
	find $(BUILDROOT) -maxdepth 1 -type d | \
		grep "\(hadoop\|zookeeper\|hive\|hbase\)" | \
		xargs -n1 basename >> $(VERSIONSFILE)

.PHONY: extract_stack_hdp
extract_stack_hdp:
	find $(BUILDROOT) -iwholename "*.tar.gz" | \
		grep "\(hadoop\|hbase\|zookeeper\|hive\)" | \
		grep -v -E "sqoop|plugin|lzo" | \
		xargs -n1 tar -C $(BUILDROOT) -xzf
	find $(BUILDROOT) -iwholename "*.tar.gz" | grep "\(tez\)" | \
		xargs sh -c 'mkdir -p $(BUILDROOT)/`basename $${0%.tar.gz}` && \
		tar -C $(BUILDROOT)/`basename $${0%.tar.gz}` -xzf $$0'
	find $(BUILDROOT) -type d -a -iname "$(HADOOP_DISTRO)-*" | xargs rm -rf
	rm -rf $(BUILDROOT)/*.tar.gz $(BUILDROOT)/hbase*/docs
	mv $(BUILDROOT)/apache-hive* $(BUILDROOT)/hive || true
	chown root:root -R $(BUILDROOT)/* || true
	find $(BUILDROOT) -maxdepth 1 -type d | \
		grep "\(hadoop\|hbase\|zookeeper\|hive\|tez\)" | \
		xargs -n1 basename >> $(VERSIONSFILE)

.PHONY: rename_tomcat
rename_tomcat:
	mv `echo $(BUILDROOT)/apache-tomcat-*` $(BUILDROOT)/tomcat

.PHONY: copy_templates
copy_templates: $(TEMPLATES)
	for X in `ls $(BUILDROOT)`; do \
		if [ -d "$(TEMPLATESROOT)/$$X" ]; \
			then cp -r $(TEMPLATESROOT)/$$X/* $(BUILDROOT)/$$X; \
		fi; \
	done;
	cp -r $(TEMPLATESROOT)/conf $(BUILDROOT)

	-find $(BUILDROOT) -iname "*~" | xargs rm -f

.PHONY: copy_docs
copy_docs: $(DOCSROOT)/README.txt
	cp $^ $(BUILDROOT)

.PHONY: copy_deps
copy_deps:
	find . -maxdepth 1 -name *.tar.gz | xargs -I {} tar xzf {} -C $(BUILDROOT)

.PHONY: refresh_tars
refresh_tars:
	make -C $(TARSROOT) clean all

.PHONY: make_tarball
make_tarball: $(BUILDROOT)
	tar czf $(BUILDROOT).tar.gz $<

%.extracted: $(TARSROOT)/%.tar.gz
	tar xzf $^ -C $(BUILDROOT)
	touch $@
	echo $* >> $(VERSIONSFILE)
