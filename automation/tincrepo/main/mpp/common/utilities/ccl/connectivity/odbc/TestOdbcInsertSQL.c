#include <stdio.h>
#include <stdlib.h>
#include "OdbcCommon.h"

int main(int argc, char *argv[])
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;

    initOdbcEnv(&env);
    int ret = getDBConnection(&env, &dbc, "ccltest");
    ASSERT_TRUE(0 == ret);

    ret = createTable(&dbc, &stmt, "CREATE TABLE odbctest (a INT);");
    ASSERT_TRUE(0 == ret);
    printf("Create table success\n");

    ret = insertData(&dbc, &stmt, "INSERT INTO odbctest VALUES (1);");
    ASSERT_TRUE(0 == ret);
    printf("Insert data into table success\n");
    
    ret = dropTable(&dbc, &stmt, "odbctest");
    ASSERT_TRUE(0 == ret);
    printf("Drop table success\n");
    
    releaseDBConnection(&dbc);
    destroyOdbcEnv(&env);

    return 0;
}
