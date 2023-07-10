---
title: 'Example: Reading From and Writing to a PostgreSQL Table'
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

- Create a PostgreSQL database and table, and insert data into the table
- Create a PostgreSQL user and assign all privileges on the table to the user
- Configure the PXF JDBC connector to access the PostgreSQL database
- Create a PXF readable external table that references the PostgreSQL table
- Read the data in the PostgreSQL table using PXF
- Create a PXF writable external table that references the PostgreSQL table
- Write data to the PostgreSQL table using PXF
- Read the data in the PostgreSQL table again

## <a id="ex_create_pgtbl"></a>Create a PostgreSQL Table

Perform the following steps to create a PostgreSQL table named `forpxf_table1` in the `public` schema of a database named `pgtestdb`, and grant a user named `pxfuser1` all privileges on this table:

1. Identify the host name and port of your PostgreSQL server.

2. Connect to the default PostgreSQL database as the `postgres` user. For example, if your PostgreSQL server is running on the default port on the host named `pserver`:

    ``` shell
    $ psql -U postgres -h pserver
    ```

3. Create a PostgreSQL database named `pgtestdb` and connect to this database:

    ``` sql
    =# CREATE DATABASE pgtestdb;
    =# \connect pgtestdb;
    ```

4. Create a table named `forpxf_table1` and insert some data into this table:

    ``` sql
    =# CREATE TABLE forpxf_table1(id int);
    =# INSERT INTO forpxf_table1 VALUES (1);
    =# INSERT INTO forpxf_table1 VALUES (2);
    =# INSERT INTO forpxf_table1 VALUES (3);
    ```

5. Create a PostgreSQL user named `pxfuser1`:

    ``` sql
    =# CREATE USER pxfuser1 WITH PASSWORD 'changeme';
    ```

6. Assign user `pxfuser1` all privileges on table `forpxf_table1`, and exit the `psql` subsystem:

    ``` sql
    =# GRANT ALL ON forpxf_table1 TO pxfuser1;
    =# \q
    ```

    With these privileges, `pxfuser1` can read from and write to the `forpxf_table1` table.

7. Update the PostgreSQL configuration to allow user `pxfuser1` to access `pgtestdb` from each Greenplum Database host. This configuration is specific to your PostgreSQL environment. You will update the `/var/lib/pgsql/pg_hba.conf` file and then restart the PostgreSQL server.


## <a id="ex_jdbconfig"></a>Configure the JDBC Connector

You must create a JDBC server configuration for PostgreSQL and synchronize the PXF configuration. The PostgreSQL JAR file is bundled with PXF, so there is no need to manually download it.

This procedure will typically be performed by the Greenplum Database administrator.

1. Log in to the Greenplum Database coordinator host:

    ``` shell
    $ ssh gpadmin@<coordinator>
    ```

2. Create a JDBC server configuration for PostgreSQL as described in [Example Configuration Procedure](jdbc_cfg.html#cfg_proc), naming the server directory `pgsrvcfg`. The `jdbc-site.xml` file contents should look similar to the following (substitute your PostgreSQL host system for `pgserverhost`):

    ``` xml
    <?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>jdbc.driver</name>
        <value>org.postgresql.Driver</value>
    </property>
    <property>
        <name>jdbc.url</name>
        <value>jdbc:postgresql://pgserverhost:5432/pgtestdb</value>
    </property>
    <property>
        <name>jdbc.user</name>
        <value>pxfuser1</value>
    </property>
    <property>
        <name>jdbc.password</name>
        <value>changeme</value>
    </property>
</configuration>
    ```

3. Synchronize the PXF server configuration to the Greenplum Database cluster:

    ``` shell
    gpadmin@coordinator$ pxf cluster sync
    ```

## <a id="ex_readjdbc"></a>Read from the PostgreSQL Table

Perform the following procedure to create a PXF external table that references the `forpxf_table1` PostgreSQL table that you created in the previous section, and reads the data in the table:

1. Create the PXF external table specifying the `jdbc` profile. For example:

    ``` sql
    gpadmin=# CREATE EXTERNAL TABLE pxf_tblfrompg(id int)
                LOCATION ('pxf://public.forpxf_table1?PROFILE=jdbc&SERVER=pgsrvcfg')
                FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
    ```

2. Display all rows of the `pxf_tblfrompg` table:

    ``` sql
    gpadmin=# SELECT * FROM pxf_tblfrompg;
     id
    ----
      1
      2
      3
    (3 rows)
    ```

## <a id="ex_writejdbc"></a>Write to the PostgreSQL Table

Perform the following procedure to insert some data into the `forpxf_table1` Postgres table and then read from the table. You must create a new external table for the write operation.

1. Create a writable PXF external table specifying the `jdbc` profile. For example:

    ``` sql
    gpadmin=# CREATE WRITABLE EXTERNAL TABLE pxf_writeto_postgres(id int)
                LOCATION ('pxf://public.forpxf_table1?PROFILE=jdbc&SERVER=pgsrvcfg')
              FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');
    ```

4. Insert some data into the `pxf_writeto_postgres` table. For example:

    ``` sql
    =# INSERT INTO pxf_writeto_postgres VALUES (111);
    =# INSERT INTO pxf_writeto_postgres VALUES (222);
    =# INSERT INTO pxf_writeto_postgres VALUES (333);
    ```

5. Use the `pxf_tblfrompg` readable external table that you created in the previous section to view the new data in the `forpxf_table1` PostgreSQL table:

    ``` sql
    gpadmin=# SELECT * FROM pxf_tblfrompg ORDER BY id DESC;
     id
    -----
     333
     222
     111
       3
       2
       1
    (6 rows)
    ```

