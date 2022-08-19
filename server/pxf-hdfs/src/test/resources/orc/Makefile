# default assumes ORC tools JAR is downloaded in current directory
# download from https://search.maven.org/artifact/org.apache.orc/orc-tools/1.7.5/jar
ORC_TOOLS_JAR ?= orc-tools-1.7.5-uber.jar

.PHONY: all
all: orc_types.orc orc_types_compound.orc orc_types_unordered_subset.orc orc_types_compound_multi.orc orc_types_repeated.orc

orc_types.orc: orc_types.json
	java -jar $(ORC_TOOLS_JAR) convert \
		--schema 'struct<t1:string,t2:string,num1:int,dub1:double,dec1:decimal(38,18),tm:timestamp,tmtz:timestamp with local time zone,r:float,bg:bigint,b:boolean,tn:tinyint,sml:smallint,dt:date,vc1:varchar(5),c1:char(3),bin:binary>' \
		--output orc_types.orc \
		--overwrite \
		orc_types.json

orc_types_unordered_subset.orc: orc_types_unordered_subset.json
	java -jar $(ORC_TOOLS_JAR) convert \
		--schema 'struct<b:boolean,num1:int,tm:timestamp,tmtz:timestamp with local time zone,vc1:varchar(5),dec1:decimal(38,18),t1:string,sml:smallint>' \
		--output orc_types_unordered_subset.orc \
		--overwrite \
		orc_types_unordered_subset.json

orc_types_compound.orc: orc_types_compound.json
	java -jar $(ORC_TOOLS_JAR) convert \
		--schema 'struct<id:int,bool_arr:array<boolean>,int2_arr:array<smallint>,int_arr:array<int>,int8_arr:array<bigint>,float_arr:array<float>,float8_arr:array<double>,text_arr:array<string>,bytea_arr:array<binary>,char_arr:array<char(15)>,varchar_arr:array<varchar(15)>,date_arr:array<date>,timestamp_arr:array<timestamp>,tmtz_arr:array<timestamp with local time zone>>' \
		--output orc_types_compound.orc \
		--overwrite \
		orc_types_compound.json

orc_types_compound_multi.orc: orc_types_compound_multi.json
	java -jar $(ORC_TOOLS_JAR) convert \
		--schema 'struct<id:int,bool_arr:array<array<boolean>>,int2_arr:array<array<smallint>>,int_arr:array<array<int>>,int8_arr:array<array<bigint>>,float_arr:array<array<float>>,float8_arr:array<array<double>>,text_arr:array<array<string>>,bytea_arr:array<array<binary>>,char_arr:array<array<char(15)>>,varchar_arr:array<array<varchar(15)>>,date_arr:array<array<date>>,timestamp_arr:array<array<timestamp>>,tmtz_arr:array<array<timestamp with local time zone>>>' \
		--output orc_types_compound_multi.orc \
		--overwrite \
		orc_types_compound_multi.json

orc_types_repeated.orc: orc_types_repeated.json
	java -jar $(ORC_TOOLS_JAR) convert \
		--schema 'struct<t1:string,t2:string,num1:int,dub1:double,dec1:decimal(38,18),tm:timestamp,tmtz:timestamp with local time zone,r:float,bg:bigint,b:boolean,tn:tinyint,sml:smallint,dt:date,vc1:varchar(5),c1:char(3),bin:binary>' \
		--output orc_types_repeated.orc \
		--overwrite \
		orc_types_repeated.json