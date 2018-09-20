
unit: tincunit tincdbunit
regress: tincregress tincdbregress
check: tinccheck tincdbcheck tincmmgrcheck
tinccheck: tincunit tincregress
tincdbcheck: tincdbunit tincdbregress
tincmmgrcheck: tincmmgrunit tincmmgrregress

tincunit:
	@echo unit test tinc
	make -C tinctest unit

tincregress:
	@echo regression test tinc
	make -C tinctest regress

tincmmgrunit:
	@echo unit test tincmmgr
	make -C tincmmgr unit

tincmmgrregress:
	@echo regression test tincmmgr
	make -C tincmmgr regress

tincdbunit:
	@echo unittesting tincdb
	make -C tincdb tincdbsetup check TINC_CONFIG=./test/tincdb_test.cfg FORCE=true

tincdbregress:
	@echo regression test tincdb
	@echo No regression tests for tincdb
