package org.greenplum.pxf.api.model;

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

import lombok.Getter;
import lombok.Setter;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

/**
 * Fragment holds a data fragment' information.
 * {@link Fragmenter#getFragments} returns a list of fragments.
 */
public class Fragment {

    public static final String[] HOSTS = new String[]{"localhost"};

    /**
     * File path+name, table name, etc.
     */
    @Getter
    private final String sourceName;

    /**
     * Fragment index (incremented per sourceName).
     */
    @Getter
    @Setter
    private int index;

    /**
     * Fragment replicas (1 or more).
     */
    @Getter
    @Setter
    private String[] replicas;

    /**
     * Fragment metadata information (starting point + length, region location, etc.).
     */
    @Getter
    @Setter
    private FragmentMetadata metadata;

    /**
     * Profile name, recommended for reading given Fragment.
     */
    @Getter
    @Setter
    private String profile;

    /**
     * Constructs a Fragment.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     */
    public Fragment(String sourceName) {
        this(sourceName, HOSTS, null);
    }

    /**
     * Constructs a Fragment.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param hosts      the replicas
     * @param metadata   the metadata for this fragment
     */
    public Fragment(String sourceName,
                    String[] hosts,
                    FragmentMetadata metadata) {
        this(sourceName, hosts, metadata, null);
    }

    /**
     * Contructs a Fragment.
     *
     * @param sourceName the resource uri (File path+name, table name, etc.)
     * @param hosts      the replicas
     * @param metadata   the metadata for this fragment
     * @param profile    the profile to use for the query
     */
    public Fragment(String sourceName,
                    String[] hosts,
                    FragmentMetadata metadata,
                    String profile) {
        this.sourceName = sourceName;
        this.replicas = hosts;
        this.metadata = metadata;
        this.profile = profile;
    }
}
