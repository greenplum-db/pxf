#include <stdio.h>
#include <stdlib.h>
#include "LibpqCommon.h"

int main(int argc, char *argv[])
{
    PGconn *conn = getDBDefaultConnection();
    ASSERT_TRUE(CONNECTION_OK == PQstatus(conn));

    PQfinish(conn);

    return 0;
}
