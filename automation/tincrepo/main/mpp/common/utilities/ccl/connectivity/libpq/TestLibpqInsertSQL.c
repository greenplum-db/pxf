#include <stdio.h>
#include <stdlib.h>
#include "LibpqCommon.h"

int main(int argc, char *argv[])
{
    PGconn *conn = getDBDefaultConnection();
    ASSERT_TRUE(CONNECTION_OK == PQstatus(conn));
   
    dropTable(conn, "libpqtable");
    createTable(conn, "CREATE TABLE libpqtable (a int)");
    printf("create table success\n");

    int num = insertData(conn, "INSERT INTO libpqtable VALUES (1)");
    ASSERT_TRUE(num == 1);
    printf("insert data into table success\n");
    
    dropTable(conn, "libpqtable");
    printf("drop table success\n");

    PQfinish(conn);

    return 0;
}
