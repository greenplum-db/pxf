package org.greenplum.pxf.plugins.hdfs.utilities;

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


import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordkeyAdapterTest {

    private RecordkeyAdapter recordkeyAdapter;

    /**
     * Test convertKeyValue for Integer type
     */
    @Test
    public void convertKeyValueInteger() {
        int key = 13;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new IntWritable(key));
    }

    /**
     * Test convertKeyValue for Boolean type
     */
    @Test
    public void convertKeyValueBoolean() {
        boolean key = true;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new BooleanWritable(key));
    }

    /**
     * Test convertKeyValue for Byte type
     */
    @Test
    public void convertKeyValueByte() {
        byte key = 1;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new ByteWritable(key));
    }

    /**
     * Test convertKeyValue for Double type
     */
    @Test
    public void convertKeyValueDouble() {
        double key = 2.3;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new DoubleWritable(key));
    }

    /**
     * Test convertKeyValue for Float type
     */
    @Test
    public void convertKeyValueFloat() {
        float key = (float) 2.3;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new FloatWritable(key));
    }

    /**
     * Test convertKeyValue for Long type
     */
    @Test
    public void convertKeyValueLong() {
        long key = 12345678901234567L;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new LongWritable(key));
    }

    /**
     * Test convertKeyValue for String type
     */
    @Test
    public void convertKeyValueString() {
        String key = "key";
        initRecordkeyAdapter();
        runConvertKeyValue(key, new Text(key));
    }

    /**
     * Test convertKeyValue for several calls of the same type
     */
    @Test
    public void convertKeyValueManyCalls() {
        boolean key = true;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new BooleanWritable(key));

        for (int i = 0; i < 5; ++i) {
            key = (i % 2) == 0;
            runConvertKeyValue(key, new BooleanWritable(key));
        }
    }

    /**
     * Test convertKeyValue for boolean type and then string type - negative
     * test
     */
    @Test
    public void convertKeyValueBadSecondValue() {
        boolean key = true;
        initRecordkeyAdapter();
        runConvertKeyValue(key, new BooleanWritable(key));
        String badKey = "bad";

        Exception e = assertThrows(ClassCastException.class,
                () -> recordkeyAdapter.convertKeyValue(badKey),
                "conversion of string to boolean should fail");
        assertTrue(e.getMessage().contains("java.lang.String cannot be cast to "));
    }

    private void initRecordkeyAdapter() {
        recordkeyAdapter = new RecordkeyAdapter();
    }

    private void runConvertKeyValue(Object key, Writable expected) {
        Writable writable = recordkeyAdapter.convertKeyValue(key);
        assertEquals(writable, expected);
    }
}
