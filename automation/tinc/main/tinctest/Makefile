TINCHOME=$(shell cd .. && pwd)
FIGLEAF_DIR=$(TINCHOME)/ext/figleaf-latest
UNIT2=$(TINCHOME)/ext/unittest2/unit2
UNIT3=$(TINCHOME)/tinctest/integration/pulse/unit3

ifdef PULSE_AGENT
    TESTER=$(UNIT2)
else
    TESTER=$(UNIT2)
endif

all:

check: unit regress

unit:
	$(TESTER) discover -s . -p "test_*.py" -v 

regress:
	$(TESTER) discover -s . -p "regress_*.py" -v


#-------------------------------------------------------------------------------
# COVERAGE
#-------------------------------------------------------------------------------
coverage:
	@echo "Running code coverage of TINC unit test"
	@$(RM) .figleaf
	@$(MAKE) -C $(TINCHOME)/ext figleaf
	@$(FIGLEAF_DIR)/bin/figleaf $(TESTER) discover -s . -p "test_*.py"
	@$(FIGLEAF_DIR)/bin/figleaf $(TESTER) discover -s . -p "regress_*.py"
	@find . -name "*.py" -not -size 0c | grep -v "integration" > pyfilelist
	@$(FIGLEAF_DIR)/bin/figleaf2html -d coverage_report .figleaf -f pyfilelist
