package org.greenplum.pxf.service.rest;

import lombok.SneakyThrows;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.List;

public class BridgeResponse implements StreamingResponseBody {

    private static final String PROFILE_KEY = "X-GP-PROFILE";

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final BridgeFactory bridgeFactory;
    private final RequestParser<MultiValueMap<String, String>> parser;
    private final RequestContext context;
    private final List<Fragment> fragments;
    private final MultiValueMap<String, String> headers;
    private final SecurityService securityService;

    public BridgeResponse(BridgeFactory bridgeFactory,
                          SecurityService securityService,
                          RequestParser<MultiValueMap<String, String>> parser,
                          RequestContext context,
                          List<Fragment> fragments,
                          MultiValueMap<String, String> headers) {
        this.securityService = securityService;
        this.bridgeFactory = bridgeFactory;
        this.parser = parser;
        this.context = context;
        this.fragments = fragments;
        this.headers = headers;
    }

    @SneakyThrows
    @Override
    public void writeTo(OutputStream out) {
        PrivilegedExceptionAction<Void> action = () -> writeToInternal(out);
        securityService.doAs(context, true, action);
    }

    private Void writeToInternal(OutputStream out) throws IOException {
        final String dataDir = context.getDataSource();
        long recordCount = 0;

        try {
            for (int i = 0; i < fragments.size(); i++) {
                RequestContext context = this.context;
                Fragment fragment = fragments.get(i);
                context.setDataSource(fragment.getSourceName());
                context.setFragmentIndex(fragment.getIndex());
                context.setFragmentMetadata(fragment.getMetadata());

                String profile = fragment.getProfile();
                if (StringUtils.isNotBlank(profile) &&
                        !StringUtils.equalsIgnoreCase(profile, context.getProfile())) {

                    LOG.debug("Using profile: {}", profile);

                    // Remember the value of the original profile defined in
                    // the table LOCATION, so we can restore it later to its
                    // original value
                    List<String> profileHeaderValue = headers.get(PROFILE_KEY);
                    headers.set(PROFILE_KEY, profile);
                    context = parser.parseRequest(headers, context.getRequestType());
                    context.setConfiguration(this.context.getConfiguration());

                    // Restore the original value of the profile if it was
                    // present
                    headers.remove(PROFILE_KEY);
                    if (profileHeaderValue != null) {
                        headers.addAll(PROFILE_KEY, profileHeaderValue);
                    }
                }

                Bridge bridge = bridgeFactory.getBridge(context);

                try {
                    if (!bridge.beginIteration()) {
                        continue;
                    }
                    Writable record;
                    DataOutputStream dos = new DataOutputStream(out);

                    LOG.debug("Starting streaming fragment {} of resource {}", fragment.getIndex(), dataDir);
                    while ((record = bridge.getNext()) != null) {
                        record.write(dos);
                        ++recordCount;
                    }
                    LOG.debug("Finished streaming fragment {} of resource {}, {} records.", fragment.getIndex(), dataDir, recordCount);
                } finally {
                    LOG.debug("Stopped streaming fragment {} of resource {}, {} records.", fragment.getIndex(), dataDir, recordCount);
                    try {
                        bridge.endIteration();
                    } catch (Exception e) {
                        LOG.warn("Ignoring error encountered during bridge.endIteration()", e);
                    }
                }
                fragments.set(i, null);
            }
        } catch (ClientAbortException e) {
            // Occurs whenever client (GPDB) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.warn(String.format("Remote connection closed by GPDB (segment %s)", context.getSegmentId()), e);
            } else {
                LOG.warn("Remote connection closed by GPDB (segment {}) (Enable debug for stacktrace)", context.getSegmentId());
            }
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }
}
