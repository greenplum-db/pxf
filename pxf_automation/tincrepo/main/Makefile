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
	$(TESTER) discover -s mpp/lib -p "test_*.py" -v 
	$(TESTER) discover -s mpp/models -p "test_*.py" -v 
	$(TESTER) discover -s mpp/gpdb/lib/models -p "test_*.py" -v 
	#$(TESTER) discover -s mpp/gpdb/lib/models -p "test_*.py" -v

regress:
	$(TESTER) discover -s mpp/models -p "regress_*.py" -v 
	$(TESTER) discover -s mpp/gpdb/lib/models -p "regress_*.py" -v 
	#no modles in gpdb currently
	#$(TESTER) discover -s mpp/gpdb/lib/models -p "regress_*.py"
