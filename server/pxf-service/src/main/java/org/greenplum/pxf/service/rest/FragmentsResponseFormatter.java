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

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.utilities.FragmentMetadataSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

/**
 * Utility class for converting Fragments into a {@link FragmentsResponse} that
 * will serialize them into JSON format.
 */
@Component
public class FragmentsResponseFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(FragmentsResponseFormatter.class);

    private final FragmentMetadataSerDe metadataSerDe;

    public FragmentsResponseFormatter(FragmentMetadataSerDe metadataSerDe) {
        this.metadataSerDe = metadataSerDe;
    }

    /**
     * Converts Fragments list to FragmentsResponse after replacing host name by
     * their respective IPs.
     *
     * @param fragments list of fragments
     * @param data      data (e.g. path) related to the fragments
     * @return FragmentsResponse with given fragments
     * @throws UnknownHostException if converting host names to IP fails
     */
    public FragmentsResponse formatResponse(List<Fragment> fragments, String data)
            throws UnknownHostException {
        /* print the raw fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            LOG.debug("Fragments before conversion to IP list:");
            printList(fragments, data);
        }

        /* HD-2550: convert host names to IPs */
        convertHostsToIPs(fragments);

        updateFragmentIndex(fragments);

        /* print the fragment list to log when in debug level */
        if (LOG.isDebugEnabled()) {
            printList(fragments, data);
        }

        return new FragmentsResponse(fragments, metadataSerDe);
    }

    /**
     * Updates the fragments' indexes so that it is incremented by sourceName.
     * (E.g.: {"a", 0}, {"a", 1}, {"b", 0} ... )
     *
     * @param fragments fragments to be updated
     */
    private void updateFragmentIndex(List<Fragment> fragments) {

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
     *
     * @throws UnknownHostException if converting host name to IP fails
     */
    private void convertHostsToIPs(List<Fragment> fragments)
            throws UnknownHostException {
        /* host converted to IP map. Used to limit network calls. */
        HashMap<String, String> hostToIpMap = new HashMap<>();

        for (Fragment fragment : fragments) {
            String[] hosts = fragment.getReplicas();
            if (hosts == null) {
                continue;
            }
            String[] ips = new String[hosts.length];
            int index = 0;

            for (String host : hosts) {
                String convertedIp = hostToIpMap.get(host);
                if (convertedIp == null) {
                    /* find host's IP, and add to map */
                    InetAddress addr = InetAddress.getByName(host);
                    convertedIp = addr.getHostAddress();
                    hostToIpMap.put(host, convertedIp);
                }

                /* update IPs array */
                ips[index] = convertedIp;
                ++index;
            }
            fragment.setReplicas(ips);
        }
    }

    /*
     * Converts a fragments list to a readable string and prints it to the log.
     * Intended for debugging purposes only. 'datapath' is the data path part of
     * the original URI (e.g., table name, *.csv, etc).
     */
    private void printList(List<Fragment> fragments, String datapath) {
        LOG.debug("List of {} fragments for \"{}\"",
                (fragments.isEmpty() ? "no" : fragments.size()), datapath);

        StringBuilder result = new StringBuilder();
        int i = 0;
        for (Fragment fragment : fragments) {
            result.setLength(0);
            result.append("Fragment #").append(++i).append(": [").append(
                    "Source: ").append(fragment.getSourceName()).append(
                    ", Index: ").append(fragment.getIndex()).append(
                    ", Replicas:");
            for (String host : fragment.getReplicas()) {
                result.append(" ").append(host);
            }

            if (fragment.getMetadata() != null) {
                result.append(", Metadata: ").append(fragment.getMetadata().toString());
            }
            result.append("] ");
            LOG.debug(result.toString());
        }
    }
}
