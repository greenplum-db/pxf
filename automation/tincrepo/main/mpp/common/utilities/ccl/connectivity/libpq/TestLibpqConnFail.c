#include <stdio.h>
#include <stdlib.h>
#include "LibpqCommon.h"

int main(int argc, char *argv[])
{
    char connectionString[1024] = {0};
    snprintf(connectionString, 1024, "hostaddr='127.0.0.1' port='5432' dbname='noexistdb' user='gpadmin'");
    
    PGconn *conn = getDBConnection(connectionString);
    ASSERT_TRUE(CONNECTION_BAD == PQstatus(conn));

    PQfinish(conn);

    return 0;
}
