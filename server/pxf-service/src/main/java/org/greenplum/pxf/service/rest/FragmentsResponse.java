package org.greenplum.pxf.service.rest;

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
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.api.utilities.FragmentMetadataSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Class for serializing fragments metadata in JSON format. The class
 * implements {@link StreamingResponseBody} so the serialization will be done
 * in a stream and not in one bulk, this in order to avoid running out of
 * memory when processing a lot of fragments.
 */
public class FragmentsResponse implements StreamingResponseBody {

    private static final Logger LOG = LoggerFactory.getLogger(FragmentsResponse.class);

    public static final byte[] PXF_FRAGMENT_ARRAY_END_BYTES = "]}".getBytes();
    public static final byte[] PXF_FRAGMENT_ARRAY_START_BYTES = "{\"PXFFragments\":[".getBytes();
    public static final String COMMA_PREFIX = ",";

    private final List<Fragment> fragments;

    /**
     * Constructs fragments response out of a list of fragments
     *
     * @param fragments fragment list
     */
    public FragmentsResponse(List<Fragment> fragments) {
        this.fragments = fragments;
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
    public void writeTo(OutputStream output) throws IOException {
        try {
            writeToInternal(output);
        } catch (ClientAbortException e) {
            // Occurs whenever client (GPDB) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.debug("Remote connection closed by GPDB", e);
            } else {
                LOG.warn("Remote connection closed by GPDB (Enable debug for stacktrace)");
            }
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private void writeToInternal(OutputStream output) throws IOException {
        DataOutputStream dos = new DataOutputStream(output);
        ObjectMapper mapper = new ObjectMapper();

        SimpleModule module = new SimpleModule();
        module.addSerializer(FragmentMetadata.class, FragmentMetadataSerDe.getInstance());
        mapper.registerModule(module);

        StringBuilder result = new StringBuilder();
        String prefix = "";

        dos.write(PXF_FRAGMENT_ARRAY_START_BYTES);
        for (Fragment fragment : fragments) {
            result.setLength(0);

            /* metaData and userData are automatically converted to Base64 */
            result.append(prefix).append(mapper.writeValueAsString(fragment));
            prefix = COMMA_PREFIX;
            dos.write(result.toString().getBytes());
        }
        dos.write(PXF_FRAGMENT_ARRAY_END_BYTES);
    }

    /**
     * @return the list of fragments for the response
     */
    public List<Fragment> getFragments() {
        return fragments;
    }
}
