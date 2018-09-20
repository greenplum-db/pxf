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
    releaseDBConnection(&dbc);
    destroyOdbcEnv(&env);

    return 0;
}
