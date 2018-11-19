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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.ReadVectorizedResolver;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utilities class exposes helper method for PXF classes
 */
public class Utilities {
    private static final Log LOG = LogFactory.getLog(Utilities.class);

    private static final String PROPERTY_KEY_USER_IMPERSONATION = "pxf.service.user.impersonation.enabled";

    /**
     * Validation for directory names that can be created
     * for the server configuration directory.
     *
     * @param name
     * @return true if valid, false otherwise
     */
    public static boolean isValidDirectoryName(String name) {
        if (StringUtils.isBlank(name) ||
                name.contains("/") ||
                name.contains("\\") ||
                name.startsWith(".") ||
                name.contains(" ") ||
                name.contains(";")) return false;
        File file = new File(name);
        try {
            file.getCanonicalPath();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Creates an object using the class name. The class name has to be a class
     * located in the webapp's CLASSPATH.
     *
     * @param confClass the class of the metaData used to initialize the
     *            instance
     * @param className a class name to be initialized.
     * @param metaData input data used to initialize the class
     * @return Initialized instance of given className
     * @throws Exception throws exception if classname was not found in
     *             classpath, didn't have expected constructor or failed to be
     *             instantiated
     */
    public static Object createAnyInstance(Class<?> confClass,
                                           String className, RequestContext metaData)
            throws Exception {

        Class<?> cls = null;
        try {
            cls = Class.forName(className);
        } catch (ClassNotFoundException e) {
            /* In case the class name uses the older and unsupported  "com.pivotal.pxf"
             * package name, recommend using the new package "org.greenplum.pxf"
             */
            if (className.startsWith("com.pivotal.pxf")) {
                throw new Exception(
                        "Class "
                                + className
                                + " does not appear in classpath. "
                                + "Plugins provided by PXF must start with \"org.greenplum.pxf\"",
                        e.getCause());
            } else {
                throw e;
            }
        }

        Constructor<?> con = cls.getConstructor(confClass);

        return instantiate(con, metaData);
    }

    /**
     * Creates an object using the class name with its default constructor. The
     * class name has to be a class located in the webapp's CLASSPATH.
     *
     * @param className a class name to be initialized
     * @return initialized instance of given className
     * @throws Exception throws exception if classname was not found in
     *             classpath, didn't have expected constructor or failed to be
     *             instantiated
     */
    public static Object createAnyInstance(String className) throws Exception {
        Class<?> cls = Class.forName(className);
        Constructor<?> con = cls.getConstructor();
        return instantiate(con);
    }

    private static Object instantiate(Constructor<?> con, Object... args)
            throws Exception {
        try {
            return con.newInstance(args);
        } catch (InvocationTargetException e) {
            /*
             * We are creating resolvers, accessors, fragmenters, etc. using the
             * reflection framework. If for example, a resolver, during its
             * instantiation - in the c'tor, will throw an exception, the
             * Resolver's exception will reach the Reflection layer and there it
             * will be wrapped inside an InvocationTargetException. Here we are
             * above the Reflection layer and we need to unwrap the Resolver's
             * initial exception and throw it instead of the wrapper
             * InvocationTargetException so that our initial Exception text will
             * be displayed in psql instead of the message:
             * "Internal Server Error"
             */
            throw (e.getCause() != null) ? new Exception(e.getCause()) : e;
        }
    }

    /**
     * Transforms a byte array into a string of octal codes in the form
     * \\xyz\\xyz
     *
     * We double escape each char because it is required in postgres bytea for
     * some bytes. In the minimum all non-printables, backslash, null and single
     * quote. Easier to just escape everything see
     * http://www.postgresql.org/docs/9.0/static/datatype-binary.html
     *
     * Octal codes must be padded to 3 characters (001, 012)
     *
     * @param bytes bytes to escape
     * @param sb octal codes of given bytes
     */
    public static void byteArrayToOctalString(byte[] bytes, StringBuilder sb) {
        if ((bytes == null) || (sb == null)) {
            return;
        }

        sb.ensureCapacity(sb.length()
                + (bytes.length * 5 /* characters per byte */));
        for (int b : bytes) {
            sb.append(String.format("\\\\%03o", b & 0xff));
        }
    }

    /**
     * Replaces any non-alpha-numeric character with a '.'. This measure is used
     * to prevent cross-site scripting (XSS) when an input string might include
     * code or script. By removing all special characters and returning a
     * censured string to the user this threat is avoided.
     *
     * @param input string to be masked
     * @return masked string
     */
    public static String maskNonPrintables(String input) {
        if (StringUtils.isEmpty(input)) {
            return input;
        }
        return input.replaceAll("[^a-zA-Z0-9_:/-]", ".");
    }

    /**
     * Parses input data and returns fragment metadata.
     *
     * @param requestContext input data which has protocol information
     * @return fragment metadata
     * @throws IllegalArgumentException if fragment metadata information wasn't found in input data
     * @throws Exception when error occurred during metadata parsing
     */
    public static FragmentMetadata parseFragmentMetadata(RequestContext requestContext) throws Exception {
        byte[] serializedLocation = requestContext.getFragmentMetadata();
        if (serializedLocation == null) {
            throw new IllegalArgumentException("Missing fragment location information");
        }
        try (ObjectInputStream objectStream = new ObjectInputStream(new ByteArrayInputStream(serializedLocation))) {
            long start = objectStream.readLong();
            long end = objectStream.readLong();
            String[] hosts = (String[]) objectStream.readObject();
            if (LOG.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("parsed file split: path ");
                sb.append(requestContext.getDataSource());
                sb.append(", start ");
                sb.append(start);
                sb.append(", end ");
                sb.append(end);
                sb.append(", hosts ");
                sb.append(ArrayUtils.toString(hosts));
                LOG.debug(sb.toString());
            }
            FragmentMetadata fragmentMetadata = new FragmentMetadata(start, end, hosts);
            return fragmentMetadata;
        } catch (Exception e) {
            LOG.error("Unable to parse fragment metadata");
            throw e;
        }
    }

    /**
     * Based on accessor information determines whether to use AggBridge
     *
     * @param requestContext input protocol data
     * @return true if AggBridge is applicable for current context
     */
    public static boolean useAggBridge(RequestContext requestContext) {
        boolean isStatsAccessor = false;
        try {
            if (requestContext == null || requestContext.getAccessor() == null) {
                throw new IllegalArgumentException("Missing accessor information");
            }
            isStatsAccessor = ArrayUtils.contains(Class.forName(requestContext.getAccessor()).getInterfaces(), StatsAccessor.class);
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to load accessor class: " + e.getMessage());
            return false;
        }
        /* Make sure filter is not present, aggregate operation supports optimization and accessor implements StatsAccessor interface */
        return (requestContext != null) && !requestContext.hasFilter()
                && (requestContext.getAggType() != null)
                && requestContext.getAggType().isOptimizationSupported()
                && requestContext.getNumAttrsProjected() == 0
                && isStatsAccessor;
    }

    /**
     * Determines whether accessor should use statistics to optimize reading results
     *
     * @param accessor accessor instance
     * @param requestContext input data which has protocol information
     * @return true if this accessor should use statistic information
     */
    public static boolean useStats(Accessor accessor, RequestContext requestContext) {
        if (accessor instanceof StatsAccessor && useAggBridge(requestContext)) {
                return true;
        } else {
            return false;
        }
    }

    /**
     * Determines whether use vectorization
     * @param requestContext input protocol data
     * @return true if vectorization is applicable in a current context
     */
    public static boolean useVectorization(RequestContext requestContext) {
        boolean isVectorizedResolver = false;
        try {
            isVectorizedResolver = ArrayUtils.contains(Class.forName(requestContext.getResolver()).getInterfaces(), ReadVectorizedResolver.class);
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to load resolver class: " + e.getMessage());
        }
        return isVectorizedResolver;
    }

    /**
     * Returns whether user impersonation has been configured as enabled.
     *
     * @return true if user impersonation is enabled, false otherwise
     */
    public static boolean isUserImpersonationEnabled() {
        return StringUtils.equalsIgnoreCase(System.getProperty(PROPERTY_KEY_USER_IMPERSONATION, ""), "true");
    }

    /**
     * Data sources are absolute data paths. Method ensures that dataSource
     * begins with '/' unless the path includes the protocol as a prefix
     *
     * @param dataSource The path to a file or directory of interest.
     *                   Retrieved from the client request.
     * @return an absolute data path
     */
    public static String absoluteDataPath(String dataSource) {
        return (dataSource.charAt(0) == '/') ? dataSource : "/" + dataSource;
    }
}
