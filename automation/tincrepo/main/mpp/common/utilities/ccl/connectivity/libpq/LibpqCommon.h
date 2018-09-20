#ifndef __LIBPQCOMMON_H__
#define __LIBPQCOMMON_H__

#include <libpq-fe.h>


#define ASSERT_TRUE(exp) \
    if ((exp)) ; else {\
        printf("[%s:%d] %s assert error\n", __FILE__, __LINE__, #exp); \
        exit(1); \
    } \

PGconn *getDBConnection(const char *connectionString);
PGconn *getDBDefaultConnection();
void createTable(PGconn *conn, const char *sql);
void dropTable(PGconn *conn, const char *tblname);
int insertData(PGconn *conn, const char *sql);

#endif
