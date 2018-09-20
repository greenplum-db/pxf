#include <stdio.h>
#include <stdlib.h>
#include "LibpqCommon.h"

#define MAX_LEN 128
#define SQL_LEN 1024

PGconn *getDBConnection(const char *connectionString)
{
    if (NULL == connectionString) {
        return NULL;
    }

    return PQconnectdb(connectionString);
}

PGconn *getDBDefaultConnection()
{
    // get environment variable
    char pghost[MAX_LEN] = "127.0.0.1";
    char pgport[MAX_LEN] = "5432";
    char pguser[MAX_LEN] = "gpadmin";
    char pgdb[MAX_LEN] = "template1";
    char *env = NULL;

    if ((env = getenv("PGHOST")) != NULL) {
        snprintf(pghost, MAX_LEN, "%s", env);
    }
    if ((env = getenv("PGPORT")) != NULL) {
        snprintf(pgport, MAX_LEN, "%s", env);
    }
    if ((env = getenv("PGUSER")) != NULL) {
        snprintf(pguser, MAX_LEN, "%s", env);
    }
    if ((env = getenv("PGDATABASE")) != NULL) {
        snprintf(pgdb, MAX_LEN, "%s", env);
    }
   
    char defaultConnectionString[1024] = {0};
    snprintf(defaultConnectionString, 1024, "host='%s' port='%s' dbname='%s' user='%s'", pghost, pgport, pgdb, pguser);
    
    return getDBConnection(defaultConnectionString);
}

void createTable(PGconn *conn, const char *sql)
{
    ASSERT_TRUE(CONNECTION_OK == PQstatus(conn));
    ASSERT_TRUE(NULL != sql);

    PGresult *result = NULL;
    result = PQexec(conn, sql);
    ASSERT_TRUE(PGRES_COMMAND_OK == PQresultStatus(result));

    PQclear(result);
}

void dropTable(PGconn *conn, const char *tblname)
{
    ASSERT_TRUE(CONNECTION_OK == PQstatus(conn));
    ASSERT_TRUE(NULL != tblname);

    PGresult *result = NULL;
    char sql[SQL_LEN] = {0};
    snprintf(sql, SQL_LEN, "DROP TABLE IF EXISTS %s", tblname);
    result = PQexec(conn, sql);
    ASSERT_TRUE(PGRES_COMMAND_OK == PQresultStatus(result));

    PQclear(result);
}

int insertData(PGconn *conn, const char *sql)
{
    ASSERT_TRUE(CONNECTION_OK == PQstatus(conn));
    ASSERT_TRUE(NULL != sql);

    PGresult *result = NULL;
    result = PQexec(conn, sql);
    ASSERT_TRUE(PGRES_COMMAND_OK == PQresultStatus(result));

    int num = atoi(PQcmdTuples(result));
        
    PQclear(result);

    return num;
}

