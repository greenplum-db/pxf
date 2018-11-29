package org.greenplum.pxf.service.bridge;

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

import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.io.Writable;
import org.greenplum.pxf.service.utilities.AnalyzeUtils;

import java.util.BitSet;

/**
 * ReadSamplingBridge wraps a ReadBridge, and returns only some of the output
 * records, based on a ratio sample. The sample to pass or discard a record is
 * done after all of the processing is completed (
 * {@code accessor -> resolver -> output builder}) to make sure there are no
 * chunks of data instead of single records. <br>
 * The goal is to get as uniform as possible sampling. This is achieved by
 * creating a bit map matching the precision of the sampleRatio, so that for a
 * ratio of 0.034, a bit-map of 1000 bits will be created, and 34 bits will be
 * set. This map is matched against each read record, discarding ones with a 0
 * bit and continuing until a 1 bit record is read.
 */
public class ReadSamplingBridge extends ReadBridge {

    private BitSet sampleBitSet;
    private int bitSetSize;
    private int curIndex;

    /**
     * C'tor - set the implementation of the bridge.
     *
     * @param context input containing sampling ratio
     * @throws Exception if the sampling ratio is wrong
     */
    public ReadSamplingBridge(RequestContext context) throws Exception {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    ReadSamplingBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) throws Exception {
        super(context, accessorFactory, resolverFactory);
        calculateBitSet(context.getStatsSampleRatio());
        this.curIndex = 0;
    }

    private void calculateBitSet(float sampleRatio) {
        int sampleSize = (int) (sampleRatio * 10000);
        bitSetSize = 10000;

        while ((bitSetSize > 100) && (sampleSize % 10 == 0)) {
            bitSetSize /= 10;
            sampleSize /= 10;
        }
        LOG.debug("bit set size = %d sample size = %d", bitSetSize, sampleSize);

        sampleBitSet = AnalyzeUtils.generateSamplingBitSet(bitSetSize, sampleSize);
    }

    /**
     * Fetches next sample, according to the sampling ratio.
     */
    @Override
    public Writable getNext() throws Exception {
        Writable output = super.getNext();

        // sample - if bit is false, advance to the next object
        while (!sampleBitSet.get(curIndex)) {

            if (output == null) {
                break;
            }
            incIndex();
            output = super.getNext();
        }

        incIndex();
        return output;
    }

    private void incIndex() {
        curIndex = (++curIndex) % bitSetSize;
    }
}
