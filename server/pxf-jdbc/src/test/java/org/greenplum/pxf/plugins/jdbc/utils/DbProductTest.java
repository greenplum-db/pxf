package org.greenplum.pxf.plugins.jdbc.utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class DbProductTest {
    private static final Date[] DATES = new Date[1];
    private static final Timestamp[] TIMESTAMPS = new Timestamp[1];
    static {
        try {
            DATES[0] = new Date(
                new SimpleDateFormat("yyyy-MM-dd").parse("2001-01-01 00:00:00").getTime()
            );
            TIMESTAMPS[0] = new Timestamp(
                new SimpleDateFormat("yyyy-MM-dd").parse("2001-01-01 00:00:00").getTime()
            );
        }
        catch (ParseException e) {
            DATES[0] = null;
            TIMESTAMPS[0] = null;
        }
    }

    private static final String DB_NAME_UNKNOWN = "no such database";

    @Test
    public void testUnknownProductIsPostgresProduct() {
        assertEquals(DbProduct.POSTGRES, DbProduct.getDbProduct(DB_NAME_UNKNOWN));
    }

    /**
     * This test also applies to Postgres database
     */
    @Test
    public void testUnknownDates() {
        final String[] EXPECTED = {"date'2001-01-01'"};

        DbProduct dbProduct = DbProduct.getDbProduct(DB_NAME_UNKNOWN);

        for (int i = 0; i < DATES.length; i++) {
            assertEquals(EXPECTED[i], dbProduct.wrapDate(DATES[i]));
        }
    }

    /**
     * This test also applies to Postgres database
     */
    @Test
    public void testUnknownTimestamps() {
        final String[] EXPECTED = {"'2001-01-01 00:00:00.0'"};

        DbProduct dbProduct = DbProduct.getDbProduct(DB_NAME_UNKNOWN);

        for (int i = 0; i < TIMESTAMPS.length; i++) {
            assertEquals(EXPECTED[i], dbProduct.wrapTimestamp(TIMESTAMPS[i]));
        }
    }


    private static final String DB_NAME_ORACLE = "ORACLE";

    @Test
    public void testOracleDates() {
        final String[] EXPECTED = {"to_date('2001-01-01', 'YYYY-MM-DD')"};

        DbProduct dbProduct = DbProduct.getDbProduct(DB_NAME_ORACLE);

        for (int i = 0; i < DATES.length; i++) {
            assertEquals(EXPECTED[i], dbProduct.wrapDate(DATES[i]));
        }
    }

    @Test
    public void testOracleTimestamps() {
        final String[] EXPECTED = {"to_timestamp('2001-01-01 00:00:00.0', 'YYYY-MM-DD HH:MI:SS.FF')"};

        DbProduct dbProduct = DbProduct.getDbProduct(DB_NAME_ORACLE);

        for (int i = 0; i < TIMESTAMPS.length; i++) {
            assertEquals(EXPECTED[i], dbProduct.wrapTimestamp(TIMESTAMPS[i]));
        }
    }


    private static final String DB_NAME_MICROSOFT = "MICROSOFT";

    @Test
    public void testMicrosoftDates() {
        final String[] EXPECTED = {"'2001-01-01'"};

        DbProduct dbProduct = DbProduct.getDbProduct(DB_NAME_MICROSOFT);

        for (int i = 0; i < DATES.length; i++) {
            assertEquals(EXPECTED[i], dbProduct.wrapDate(DATES[i]));
        }
    }


    private static final String DB_NAME_MYSQL = "MYSQL";

    @Test
    public void testMySQLDates() {
        final String[] EXPECTED = {"DATE('2001-01-01')"};

        DbProduct dbProduct = DbProduct.getDbProduct(DB_NAME_MYSQL);

        for (int i = 0; i < DATES.length; i++) {
            assertEquals(EXPECTED[i], dbProduct.wrapDate(DATES[i]));
        }
    }
}
