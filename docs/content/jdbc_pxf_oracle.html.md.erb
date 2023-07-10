---
title: 'Example: Reading From and Writing to an Oracle Table'
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

In this example, you:

- Create an Oracle user and assign all privileges on the table to the user
- Create an Oracle table, and insert data into the table
- Configure the PXF JDBC connector to access the Oracle database
- Create a PXF readable external table that references the Oracle table
- Read the data in the Oracle table using PXF
- Create a PXF writable external table that references the Oracle table
- Write data to the Oracle table using PXF
- Read the data in the Oracle table again

For information about controlling parallel execution in Oracle, refer to [About Setting Parallel Query Session Parameters](#parallel) located at the end of this topic.

## <a id="ex_create_pgtbl"></a>Create an Oracle Table

Perform the following steps to create an Oracle table named `countries` in the schema `oracleuser`, and grant a user named `oracleuser` all the necessary privileges:

1. Identify the host name and port of your Oracle server.

2. Connect to the Oracle database as the `system` user:

    ``` shell
    $ sqlplus system
    ```

3. Create a user named `oracleuser` and assign the password `mypassword` to it:

    ``` sql
    > CREATE USER oracleuser IDENTIFIED BY mypassword;
    ```

4. Assign user `oracleuser` enough privileges to login, create and modify a table:

    ``` sql
    > GRANT CREATE SESSION TO oracleuser; 
    > GRANT CREATE TABLE TO oracleuser;
    > GRANT UNLIMITED TABLESPACE TO oracleuser;
    > exit
    ```

4. Log in as user `oracleuser`:

    ``` shell
    $ sqlplus oracleuser
    ```

4. Create a table named `countries`, insert some data into this table, and commit the transaction:

    ``` sql
    > CREATE TABLE countries (country_id int, country_name varchar(40), population float);
    > INSERT INTO countries (country_id, country_name, population) values (3, 'Portugal', 10.28);
    > INSERT INTO countries (country_id, country_name, population) values (24, 'Zambia', 17.86);
    > COMMIT;
    ```

## <a id="ex_jdbconfig"></a>Configure the Oracle Connector

You must create a JDBC server configuration for Oracle, download the Oracle driver JAR file to your system, copy the JAR file to the PXF user configuration directory, synchronize the PXF configuration, and then restart PXF.

This procedure will typically be performed by the Greenplum Database administrator.

1. Download the Oracle JDBC driver and place it under `$PXF_BASE/lib` of your Greenplum Database coordinator host. If you [relocated $PXF_BASE](about_pxf_dir.html#movebase), make sure you use the updated location. You can download a Oracle JDBC driver from your preferred download location. The following example places a driver downloaded from Oracle webiste under `$PXF_BASE/lib` of the Greenplum Database coordinator:

    1. If you did not relocate `$PXF_BASE`, run the following from the Greenplum coordinator:

        ```shell
        gpadmin@coordinator$ scp ojdbc10.jar gpadmin@coordinator:/usr/local/pxf-gp<version>/lib/
        ```

    2. If you relocated `$PXF_BASE`, run the following from the Greenplum coordinator:

        ```shell
        gpadmin@coordinator$ scp ojdbc10.jar gpadmin@coordinator:$PXF_BASE/lib/
        ```

1. Synchronize the PXF configuration, and then restart PXF: 

    ```shell
    gpadmin@coordinator$ pxf cluster sync
    gpadmin@coordinator$ pxf cluster restart
    ```

2. Create a JDBC server configuration for Oracle as described in [Example Configuration Procedure](jdbc_cfg.html#cfg_proc), naming the server directory `oracle`. The `jdbc-site.xml` file contents should look similar to the following (substitute your Oracle host system for `oracleserverhost`, and the value of your Oracle service name for `orcl`):

    ``` xml
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
        <property>
            <name>jdbc.driver</name>
            <value>oracle.jdbc.driver.OracleDriver</value>
            <description>Class name of the JDBC driver</description>
        </property>
        <property>
            <name>jdbc.url</name>
            <value>jdbc:oracle:thin:@oracleserverhost:1521/orcl</value>
            <description>The URL that the JDBC driver can use to connect to the database</description>
        </property>
        <property>
            <name>jdbc.user</name>
            <value>oracleuser</value>
            <description>User name for connecting to the database</description>
        </property>
        <property>
            <name>jdbc.password</name>
            <value>mypassword</value>
            <description>Password for connecting to the database</description>
        </property>
    </configuration>  
    ```

3. Synchronize the PXF server configuration to the Greenplum Database cluster:

    ``` shell
    gpadmin@coordinator$ pxf cluster sync
    ```

## <a id="ex_readjdbc"></a>Read from the Oracle Table

Perform the following procedure to create a PXF external table that references the `countries` Oracle table that you created in the previous section, and reads the data in the table:

1. Create the PXF external table specifying the `jdbc` profile. For example:

    ``` sql
    gpadmin=# CREATE EXTERNAL TABLE oracle_countries (country_id int, country_name varchar, population float)
              LOCATION('pxf://oracleuser.countries?PROFILE=jdbc&SERVER=oracle')
              FORMAT 'CUSTOM' (formatter='pxfwritable_import');
    ```

2. Display all rows of the `oracle_countries` table:

    ``` sql
    gpadmin=# SELECT * FROM oracle_countries ;
    country_id | country_name | population 
    -----------+--------------+------------
             3 | Portugal     |      10.28
            24 | Zambia       |      17.86
    (2 rows)
    ```

## <a id="ex_writejdbc"></a>Write to the Oracle Table

Perform the following procedure to insert some data into the `countries` Oracle table and then read from the table. You must create a new external table for the write operation.

1. Create a writable PXF external table specifying the `jdbc` profile. For example:

    ``` sql
    gpadmin=# CREATE WRITABLE EXTERNAL TABLE oracle_countries_write (country_id int, country_name varchar, population float)
              LOCATION('pxf://oracleuser.countries?PROFILE=jdbc&SERVER=oracle')
              FORMAT 'CUSTOM' (formatter='pxfwritable_export');
    ```

4. Insert some data into the `oracle_countries_write` table. For example:

    ``` sql
    gpadmin=# INSERT INTO oracle_countries_write VALUES (66, 'Colombia', 50.34);
    ```

5. Use the `oracle_countries` readable external table that you created in the previous section to view the new data in the `countries` Oracle table:

    ``` sql
    gpadmin=#  SELECT * FROM oracle_countries;
    country_id | country_name | population
    ------------+--------------+------------
             3 | Portugal     |      10.28
            24 | Zambia       |      17.86
            66 | Colombia     |      50.34
    (3 rows)
    ```


## <a id="parallel"></a>About Setting Oracle Parallel Query Session Parameters

PXF recognizes certain Oracle session parameters that control parallel query execution, and will set these parameters before it runs a query. You specify these session parameters via properties that you set in the `jdbc-site.xml` configuration file for the Oracle PXF server.

For more information about parallel query execution in Oracle databases, refer to the [Oracle documentation](https://docs.oracle.com/database/121/VLDBG/GUID-3E2AE088-2505-465E-A8B2-AC38813EA355.htm#VLDBG010).

PXF names an Oracle parallel query session property as follows:

```
jdbc.session.property.alter_session_parallel.<n>
```

`<n>` is an ordinal number that identifies a session parameter setting; for example, `jdbc.session.property.alter_session_parallel.1`. You may specify multiple property settings, where `<n>` is unique in each.

A value that you specify for an Oracle parallel query execution property must conform to the following format:

```
<action>.<statement_type>[.<degree_of_parallelism>]
```

where:

| Keyword | Values/Description |
|--------------|-----------------|
| `<action>`  | `enable`</br>`disable`</br>`force` |
| `<statement_type>`  | `query`</br>`ddl`</br>`dml`</br> |
| `<degree_of_parallelism>`  | The \(integer\) number of parallel sessions that you can force when `<action>` specifies `force`. PXF ignores this value for other `<action>` settings. |

Example parallel query execution property settings in the `jdbc-site.xml` configuration file for an Oracle PXF server follow:

``` xml
<property>
    <name>jdbc.session.property.alter_session_parallel.1</name>
    <value>force.query.4</value>
</property>
<property>
    <name>jdbc.session.property.alter_session_parallel.2</name>
    <value>disable.ddl</value>
</property>
<property>
    <name>jdbc.session.property.alter_session_parallel.3</name>
    <value>enable.dml</value>
</property>
```

With this configuration, PXF runs the following commands before it submits the query to the Oracle database:

``` sql
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 4;
ALTER SESSION DISABLE PARALLEL DDL;
ALTER SESSION ENABLE PARALLEL DML;
```

