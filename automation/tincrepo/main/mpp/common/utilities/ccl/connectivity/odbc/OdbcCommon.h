#ifndef __ODBCCOMMON_H__
#define __ODBCCOMMON_H__

#include <sql.h>
#include <sqlext.h>

#define ASSERT_TRUE(exp) \
	if ((exp)) ; else {\
		printf("[%s:%d] %s assert error\n", __FILE__, __LINE__, #exp); \
		exit(1); \
	} \


void initOdbcEnv(SQLHENV *env);
void destroyOdbcEnv(SQLHENV *env);

int getDBConnection(SQLHENV *env, SQLHDBC *dbc, SQLCHAR *dsn);
void releaseDBConnection(SQLHDBC *dbc);

int createTable(SQLHDBC *dbc, SQLHSTMT *stmt, SQLCHAR *sql);
int dropTable(SQLHDBC *dbc, SQLHSTMT *stmt, SQLCHAR *tablename);
int insertData(SQLHDBC *dbc, SQLHSTMT *stmt, SQLCHAR *sql);

#endif
