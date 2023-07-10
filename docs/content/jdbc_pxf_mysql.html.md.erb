---
title: 'Example: Reading From and Writing to a MySQL Table'
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

- Create a MySQL database and table, and insert data into the table
- Create a MySQL user and assign all privileges on the table to the user
- Configure the PXF JDBC connector to access the MySQL database
- Create a PXF readable external table that references the MySQL table
- Read the data in the MySQL table using PXF
- Create a PXF writable external table that references the MySQL table
- Write data to the MySQL table using PXF
- Read the data in the MySQL table again

## <a id="ex_create_pgtbl"></a>Create a MySQL Table

Perform the following steps to create a MySQL table named `names` in a database named `mysqltestdb`, and grant a user named `mysql-user` all privileges on this table:

1. Identify the host name and port of your MySQL server.

2. Connect to the default MySQL database as the `root` user:

    ``` shell
    $ mysql -u root -p
    ```

3. Create a MySQL database named `mysqltestdb` and connect to this database:

    ``` sql
    > CREATE DATABASE mysqltestdb;
    > USE mysqltestdb;
    ```

4. Create a table named `names` and insert some data into this table:

    ``` sql
    > CREATE TABLE names (id int, name varchar(64), last varchar(64));
    > INSERT INTO names values (1, 'John', 'Smith'), (2, 'Mary', 'Blake');
    ```

5. Create a MySQL user named `mysql-user` and assign the password `my-secret-pw` to it:

    ``` sql
    > CREATE USER 'mysql-user' IDENTIFIED BY 'my-secret-pw';
    ```

6. Assign user `mysql-user` all privileges on table `names`, and exit the `mysql` subsystem:

    ``` sql
    > GRANT ALL PRIVILEGES ON mysqltestdb.names TO 'mysql-user';
    > exit
    ```

    With these privileges, `mysql-user` can read from and write to the `names` table.

## <a id="ex_jdbconfig"></a>Configure the MySQL Connector

You must create a JDBC server configuration for MySQL, download the MySQL driver JAR file to your system, copy the JAR file to the PXF user configuration directory, synchronize the PXF configuration, and then restart PXF.

This procedure will typically be performed by the Greenplum Database administrator.

1. Log in to the Greenplum Database coordinator host:

    ``` shell
    $ ssh gpadmin@<coordinator>
    ```
1. Download the MySQL JDBC driver and place it under `$PXF_BASE/lib`. If you [relocated $PXF_BASE](about_pxf_dir.html#movebase), make sure you use the updated location. You can download a MySQL JDBC driver from your preferred download location. The following example downloads the driver from Maven Central and places it under `$PXF_BASE/lib`:

    1. If you did not relocate `$PXF_BASE`, run the following from the Greenplum coordinator:

        ```shell
        gpadmin@gcoord$ cd /usr/local/pxf-gp<version>/lib
        gpadmin@coordinator$ wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.21/mysql-connector-java-8.0.21.jar
        ```

    2. If you relocated `$PXF_BASE`, run the following from the Greenplum coordinator:

        ```shell
        gpadmin@coordinator$ cd $PXF_BASE/lib
        gpadmin@coordinator$ wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.21/mysql-connector-java-8.0.21.jar
        ```

1. Synchronize the PXF configuration, and then restart PXF:

    ```shell
    gpadmin@coordinator$ pxf cluster sync
    gpadmin@coordinator$ pxf cluster restart
    ```

2. Create a JDBC server configuration for MySQL as described in [Example Configuration Procedure](jdbc_cfg.html#cfg_proc), naming the server directory `mysql`. The `jdbc-site.xml` file contents should look similar to the following (substitute your MySQL host system for `mysqlserverhost`):

    ``` xml
    <?xml version="1.0" encoding="UTF-8"?>
    <configuration>
        <property>
            <name>jdbc.driver</name>
            <value>com.mysql.jdbc.Driver</value>
            <description>Class name of the JDBC driver</description>
        </property>
        <property>
            <name>jdbc.url</name>
            <value>jdbc:mysql://mysqlserverhost:3306/mysqltestdb</value>
            <description>The URL that the JDBC driver can use to connect to the database</description>
        </property>
        <property>
            <name>jdbc.user</name>
            <value>mysql-user</value>
            <description>User name for connecting to the database</description>
        </property>
        <property>
            <name>jdbc.password</name>
            <value>my-secret-pw</value>
            <description>Password for connecting to the database</description>
        </property>
    </configuration> 
    ```

3. Synchronize the PXF server configuration to the Greenplum Database cluster:

    ``` shell
    gpadmin@coordinator$ pxf cluster sync
    ```

## <a id="ex_readjdbc"></a>Read from the MySQL Table

Perform the following procedure to create a PXF external table that references the `names` MySQL table that you created in the previous section, and reads the data in the table:

1. Create the PXF external table specifying the `jdbc` profile. For example:

    ``` sql
    gpadmin=# CREATE EXTERNAL TABLE names_in_mysql (id int, name text, last text)
              LOCATION('pxf://names?PROFILE=jdbc&SERVER=mysql')
              FORMAT 'CUSTOM' (formatter='pxfwritable_import');
    ```

2. Display all rows of the `names_in_mysql` table:

    ``` sql
    gpadmin=# SELECT * FROM names_in_mysql;
     id |   name    |   last  
    ----+-----------+----------
      1 |   John    |   Smith
      2 |   Mary    |   Blake
    (2 rows)   
    ```

## <a id="ex_writejdbc"></a>Write to the MySQL Table

Perform the following procedure to insert some data into the `names` MySQL table and then read from the table. You must create a new external table for the write operation.

1. Create a writable PXF external table specifying the `jdbc` profile. For example:

    ``` sql
    gpadmin=# CREATE WRITABLE EXTERNAL TABLE names_in_mysql_w (id int, name text, last text)
              LOCATION('pxf://names?PROFILE=jdbc&SERVER=mysql')
              FORMAT 'CUSTOM' (formatter='pxfwritable_export');
    ```

4. Insert some data into the `names_in_mysql_w` table. For example:

    ``` sql
    =# INSERT INTO names_in_mysql_w VALUES (3, 'Muhammad', 'Ali');
    ```

5. Use the `names_in_mysql` readable external table that you created in the previous section to view the new data in the `names` MySQL table:

    ``` sql
    gpadmin=#  SELECT * FROM names_in_mysql;
     id |   name     |   last  
    ----+------------+--------
      1 |   John     |   Smith
      2 |   Mary     |   Blake
      3 |   Muhammad |   Ali
    (3 rows)
  
