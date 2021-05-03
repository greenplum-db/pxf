package org.greenplum.pxf.plugins.hive.utilities;

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

import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFilter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProfileFactoryTest {

    @Test
    public void get() {

        // if user specified vectorized ORC, no matter what the input format is, the profile (as in parameter) should be used
        String profileName = ProfileFactory.get(new TextInputFormat(), false, "HiveVectorizedORC");
        assertEquals("HiveVectorizedORC", profileName);
        profileName = ProfileFactory.get(new TextInputFormat(), false, "hivevectorizedorc");
        assertEquals("hivevectorizedorc", profileName);

        // For TextInputFormat when table has no complex types, HiveText profile should be used
        profileName = ProfileFactory.get(new TextInputFormat(), false);
        assertEquals("hive:text", profileName);

        // For TextInputFormat when table has complex types, Hive profile should be used, HiveText doesn't support complex types yet
        profileName = ProfileFactory.get(new TextInputFormat(), true);
        assertEquals("hive", profileName);

        // For RCFileInputFormat when table has complex types, HiveRC profile should be used
        profileName = ProfileFactory.get(new RCFileInputFormat(), true);
        assertEquals("hive:rc", profileName);

        // For RCFileInputFormat when table has no complex types, HiveRC profile should be used
        profileName = ProfileFactory.get(new RCFileInputFormat(), false);
        assertEquals("hive:rc", profileName);

        // For OrcInputFormat when table has complex types, HiveORC profile should be used
        profileName = ProfileFactory.get(new OrcInputFormat(), true);
        assertEquals("hive:orc", profileName);

        // For OrcInputFormat when table has no complex types, HiveORC profile should be used
        profileName = ProfileFactory.get(new OrcInputFormat(), false);
        assertEquals("hive:orc", profileName);

        // For other formats Hive profile should be used
        profileName = ProfileFactory.get(new SequenceFileInputFilter(), false);
        assertEquals("hive", profileName);
    }

}
