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

import org.greenplum.pxf.api.model.Fragment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Utility class for converting Fragments into a {@link FragmentsResponse} that
 * will serialize them into JSON format.
 */
public class FragmentsResponseFormatter {

    private static final Log LOG = LogFactory.getLog(FragmentsResponseFormatter.class);

    /**
     * Converts Fragments list to FragmentsResponse after replacing host name by
     * their respective IPs.
     *
     * @param fragments list of fragments
     * @param data      data (e.g. path) related to the fragments
     * @return FragmentsResponse with given fragments
     */
    public static FragmentsResponse formatResponse(List<Fragment> fragments,
                                                   String data) {
        /* print the raw fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            LOG.debug("Fragments before conversion to IP list:");
            FragmentsResponseFormatter.printList(fragments, data);
        }

        /* HD-2550: convert host names to IPs */
        convertHostsToIPs(fragments);

        updateFragmentIndex(fragments);

        /* print the fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            FragmentsResponseFormatter.printList(fragments, data);
        }

        return new FragmentsResponse(fragments);
    }

    /**
     * Updates the fragments' indexes so that it is incremented by sourceName.
     * (E.g.: {"a", 0}, {"a", 1}, {"b", 0} ... )
     *
     * @param fragments fragments to be updated
     */
    private static void updateFragmentIndex(List<Fragment> fragments) {

        String sourceName = null;
        int index = 0;
        for (Fragment fragment : fragments) {

            String currentSourceName = fragment.getSourceName();
            if (!currentSourceName.equals(sourceName)) {
                index = 0;
                sourceName = currentSourceName;
            }
            fragment.setIndex(index++);
        }
    }

    /**
     * Converts hosts to their matching IP addresses.
     */
    private static void convertHostsToIPs(List<Fragment> fragments) {
        for (Fragment fragment : fragments) {
            // We hardcode the IP address to 127.0.0.1 since this information
            // is no longer used. We avoid performing a reverse DNS lookup,
            // which can be expensive, so we skip doing unnecessary work.
            fragment.setReplicas(new String[]{"127.0.0.1"});
        }
    }

    /*
     * Converts a fragments list to a readable string and prints it to the log.
     * Intended for debugging purposes only. 'datapath' is the data path part of
     * the original URI (e.g., table name, *.csv, etc).
     */
    private static void printList(List<Fragment> fragments, String datapath) {
        LOG.debug("List of " + (fragments.isEmpty() ? "no" : fragments.size())
                + "fragments for \"" + datapath + "\"");

        int i = 0;
        for (Fragment fragment : fragments) {
            StringBuilder result = new StringBuilder();
            result.append("Fragment #").append(++i).append(": [").append(
                    "Source: ").append(fragment.getSourceName()).append(
                    ", Index: ").append(fragment.getIndex()).append(
                    ", Replicas:");
            for (String host : fragment.getReplicas()) {
                result.append(" ").append(host);
            }

            if (fragment.getMetadata() != null) {
                result.append(", Metadata: ").append(
                        new String(fragment.getMetadata()));
            }

            if (fragment.getUserData() != null) {
                result.append(", User Data: ").append(
                        new String(fragment.getUserData()));
            }
            result.append("] ");
            LOG.debug(result);
        }
    }
}
