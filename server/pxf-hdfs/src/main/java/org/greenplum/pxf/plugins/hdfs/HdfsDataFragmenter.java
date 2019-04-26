package org.greenplum.pxf.plugins.hdfs;

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


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Fragmenter class for HDFS data resources.
 * <p>
 * Given an HDFS data source (a file, directory, or wild card pattern) divide
 * the data into fragments and return a list of them along with a list of
 * host:port locations for each.
 */
public class HdfsDataFragmenter extends BaseFragmenter {

    protected JobConf jobConf;
    private HcfsType hcfsType;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
        jobConf = new JobConf(configuration, this.getClass());
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        String filename = hcfsType.getDataUri(configuration, context);
        if("true".equalsIgnoreCase(context.getOption("FILE_TUPLE")))
            getFragmentsByFile(filename);
        else
            getFragmentsBySplit(filename);

        return fragments;
    }

    /**
     * Each fragment is a split
     */
    private void getFragmentsBySplit(String filename) throws Exception {
        List<InputSplit> splits = getSplits(new Path(filename));

        for (InputSplit split : splits) {
            FileSplit fsp = (FileSplit) split;
            String filepath = fsp.getPath().toString();
            /*
             * metadata information includes: file split's start, length and
             * hosts (locations).
             */
            byte[] fragmentMetadata = HdfsUtilities.prepareFragmentMetadata(fsp);
            Fragment fragment = new Fragment(filepath, fsp.getLocations(), fragmentMetadata);
            fragments.add(fragment);
        }
        LOG.debug("Total number of fragments = {}", splits.size());
    }

    /**
     * Each fragment is a file
     */
    private void getFragmentsByFile(String filename) throws Exception {
        /*
         * For S3, the hosts is always localhost on the API call.
         * No need to calculate it, we can just hardcode it.
         */
        String[] hosts = {"localhost"};

        FileSystem fs = FileSystem.get(URI.create(filename), configuration);
        RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                fs.listFiles(new Path(filename), false);

        while (fileStatusListIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            String sourceName = fileStatus.getPath().toUri().toString();
            Fragment fragment = new Fragment(sourceName, hosts, null);
            fragments.add(fragment);
        }
        LOG.debug("Total number of fragments = {}", fragments.size());
    }

    @Override
    public FragmentStats getFragmentStats() throws Exception {
        String absoluteDataPath = hcfsType.getDataUri(configuration, context);
        ArrayList<InputSplit> splits = getSplits(new Path(absoluteDataPath));

        if (splits.isEmpty()) {
            return new FragmentStats(0, 0, 0);
        }
        long totalSize = 0;
        for (InputSplit split : splits) {
            totalSize += split.getLength();
        }
        InputSplit firstSplit = splits.get(0);
        return new FragmentStats(splits.size(), firstSplit.getLength(), totalSize);
    }

    private ArrayList<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat fformat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        InputSplit[] splits = fformat.getSplits(jobConf, 1);
        ArrayList<InputSplit> result = new ArrayList<>();

        /*
         * HD-2547: If the file is empty, an empty split is returned: no
         * locations and no length.
         */
        if (splits != null) {
            for (InputSplit split : splits) {
                if (split.getLength() > 0) {
                    result.add(split);
                }
            }
        }

        return result;
    }
}
