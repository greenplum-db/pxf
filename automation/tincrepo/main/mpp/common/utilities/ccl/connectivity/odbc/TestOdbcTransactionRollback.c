#include <stdio.h>
#include <stdlib.h>
#include "OdbcCommon.h"

int main(int argc, char *argv[])
{
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN retcode;

    initOdbcEnv(&env);
    int ret = getDBConnection(&env, &dbc, "ccltest");
    ASSERT_TRUE(0 == ret);

    ret = createTable(&dbc, &stmt, "CREATE TABLE odbctest (a INT);");
    ASSERT_TRUE(0 == ret);
    printf("Create table success\n");

    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF, 0);
    printf("Set autocommit off\n");

    ret = insertData(&dbc, &stmt, "INSERT INTO odbctest VALUES (1);");
    ASSERT_TRUE(0 == ret);
    printf("Insert data into table success\n");
    
    ret = insertData(&dbc, &stmt, "INSERT INTO odbctest VALUES (2);");
    ASSERT_TRUE(0 == ret);
    printf("Insert data into table success\n");
 
    SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
    printf("Transaction rollback\n");

    SQLSetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_ON, 0);
    printf("Set autocommit on\n");
    
    // Execute not support SQL
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    SQLCHAR sql[1024];
    snprintf(sql, sizeof(sql), "%s", "SELECT a FROM odbctest ORDER BY a;");
    ret = SQLExecDirect(stmt, sql, SQL_NTS);
    ASSERT_TRUE(0 == ret);

    int num = 0;
    while (SQL_SUCCEEDED(retcode = SQLFetch(stmt))) {
        SQLINTEGER data;
        SQLINTEGER indicator;
        retcode = SQLGetData(stmt, 1, SQL_C_ULONG, &data, 0, &indicator);
        printf("data : %d\n", data);
        num++;
    }
    ASSERT_TRUE(0 == num);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    printf("Select table success\n");

    ret = dropTable(&dbc, &stmt, "odbctest");
    ASSERT_TRUE(0 == ret);
    printf("Drop table success\n");
 
    releaseDBConnection(&dbc);
    destroyOdbcEnv(&env);

    return 0;
}
