#include <stdio.h>
#include <stdlib.h>
#include "OdbcCommon.h"

void initOdbcEnv(SQLHENV *env) 
{
    // Allocate an environment handle
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env);
    SQLSetEnvAttr(*env, SQL_ATTR_ODBC_VERSION, (void *)SQL_OV_ODBC3, 0);
}

void destroyOdbcEnv(SQLHENV *env)
{
    // Free up allocated handles
    SQLFreeHandle(SQL_HANDLE_ENV, *env);
}

int getDBConnection(SQLHENV *env, SQLHDBC *dbc, SQLCHAR *dsn)
{
    // Allocate a connection handle
    SQLAllocHandle(SQL_HANDLE_DBC, *env, dbc);
    
    // Connect to the DSN
    SQLCHAR dsn_name[1024];
    snprintf(dsn_name, sizeof(dsn_name), "DSN=%s", dsn);
    
    SQLCHAR outstr[1024];
    SQLSMALLINT outstrlen;

    SQLRETURN ret = SQLDriverConnect(*dbc, NULL, dsn_name, SQL_NTS, 
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (SQL_SUCCEEDED(ret)) {
        printf("Connect successfully. DSN=%s\n", dsn); 
        printf("Returned connection string was:\n\t%s\n", outstr);
        return 0;

    } else {
        printf("Connect failed. DSN=%s\n", dsn);
        return -1;
    }
}

void releaseDBConnection(SQLHDBC *dbc)
{
    if (dbc) {
        SQLDisconnect(*dbc);
        SQLFreeHandle(SQL_HANDLE_DBC, *dbc);
    }
}

int createTable(SQLHDBC *dbc, SQLHSTMT *stmt, SQLCHAR *sql)
{
    SQLRETURN ret;
    SQLAllocHandle(SQL_HANDLE_STMT, *dbc, stmt);
    ret = SQLExecDirect(*stmt, sql, SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, *stmt);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return 0;
    } else {
        return -1;
    }
}

int dropTable(SQLHDBC *dbc, SQLHSTMT *stmt, SQLCHAR *tablename)
{
    SQLRETURN ret;
    SQLAllocHandle(SQL_HANDLE_STMT, *dbc, stmt);
    SQLCHAR sql[1024];
    snprintf(sql, sizeof(sql), "DROP TABLE %s;", tablename);
    ret = SQLExecDirect(*stmt, sql, SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, *stmt);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return 0;
    } else {
        return -1;
    }
}

int insertData(SQLHDBC *dbc, SQLHSTMT *stmt, SQLCHAR *sql)
{
    SQLRETURN ret;
    SQLAllocHandle(SQL_HANDLE_STMT, *dbc, stmt);
    ret = SQLExecDirect(*stmt, sql, SQL_NTS);
    SQLFreeHandle(SQL_HANDLE_STMT, *stmt);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return 0;
    } else {
        return -1;
    }
}


