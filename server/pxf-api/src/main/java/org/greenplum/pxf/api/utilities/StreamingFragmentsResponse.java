package org.greenplum.pxf.api.utilities;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.greenplum.pxf.api.model.StreamingFragmenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class for serializing fragments metadata in JSON format. The class implements
 * {@link StreamingOutput} so the serialization will be done in a stream and not
 * in one bulk, this in order to avoid running out of memory when processing a
 * lot of fragments.
 */
public class StreamingFragmentsResponse implements StreamingOutput {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private StreamingFragmenter fragmenter;

    public StreamingFragmentsResponse(StreamingFragmenter fragmenter) throws Exception {
        this.fragmenter = fragmenter;
        fragmenter.open();
    }

    /**
     * Serializes a fragments list in JSON, To be used as the result string for
     * GPDB. An example result is as follows:
     * <code>{"PXFFragments":[{"replicas":
     * ["sdw1.corp.emc.com","sdw3.corp.emc.com","sdw8.corp.emc.com"],
     * "sourceName":"text2.csv", "index":"0","metadata":"&lt;base64 metadata for fragment&gt;",
     * "userData":"&lt;data_specific_to_third_party_fragmenter&gt;"
     * },{"replicas":["sdw2.corp.emc.com","sdw4.corp.emc.com","sdw5.corp.emc.com"
     * ],"sourceName":"text_data.csv","index":"0","metadata":
     * "&lt;base64 metadata for fragment&gt;"
     * ,"userData":"&lt;data_specific_to_third_party_fragmenter&gt;"
     * }]}</code>
     */
    @Override
    public void write(OutputStream output) throws IOException, WebApplicationException {
        DataOutputStream dos = new DataOutputStream(output);
        ObjectMapper mapper = new ObjectMapper();

        dos.write("{\"PXFFragments\":[".getBytes());

        String prefix = "";
        while (fragmenter.hasNext()) {
            dos.write((prefix + mapper.writeValueAsString(fragmenter.next())).getBytes());
            prefix = ",";
        }
        dos.write("]}".getBytes());
    }
}
