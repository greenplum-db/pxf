package org.greenplum.pxf.service.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.MetricsReporter;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of the WriteService.
 */
@Service
@Slf4j
public class WriteServiceImpl extends BaseServiceImpl implements WriteService {

    /**
     * Creates a new instance.
     *
     * @param configurationFactory configuration factory
     * @param bridgeFactory        bridge factory
     * @param securityService      security service
     */
    public WriteServiceImpl(ConfigurationFactory configurationFactory,
                            BridgeFactory bridgeFactory,
                            SecurityService securityService,
                            MetricsReporter metricsReporter) {
        super("Write", configurationFactory, bridgeFactory, securityService, metricsReporter);
    }

    @Override
    public String writeData(RequestContext context, InputStream inputStream) throws IOException {
        OperationStats stats = processData(context, () -> readStream(context, inputStream));

        String censuredPath = Utilities.maskNonPrintables(context.getDataSource());
        String returnMsg = String.format("wrote %d bulks to %s", stats.getRecordCount(), censuredPath);
        log.debug("{} {}", context.getId(), returnMsg);

        return returnMsg;
    }

    /**
     * Reads the input stream, iteratively submits submits data from the stream to created bridge.
     *
     * @param context     request context
     * @param inputStream input stream
     * @return operation statistics
     * @throws IOException if error occurs when writing data
     */
    private OperationStats readStream(RequestContext context, InputStream inputStream) throws IOException {
        Bridge bridge = getBridge(context);

        long recordCount = 0;
        IOException ex = null;

        // dataStream (and inputStream as the result) will close automatically at the end of the try block
        try (DataInputStream dataStream = new DataInputStream(inputStream)) {
            // open the output file
            bridge.beginIteration();
            while (bridge.setNext(dataStream)) {
                ++recordCount;
            }
        } catch (ClientAbortException cae) {
            // Occurs whenever client (GPDB) decides to end the connection
            if (log.isDebugEnabled()) {
                // Stacktrace in debug
                log.warn(String.format("%s Remote connection closed by GPDB (segment %s)",
                        context.getId(), context.getSegmentId()), cae);
            } else {
                log.warn("{} Remote connection closed by GPDB (segment {}) (Enable debug for stacktrace)",
                        context.getId(), context.getSegmentId());
            }
            ex = cae;
            // Re-throw the exception so Spring MVC is aware that an IO error has occurred
            throw cae;
        } catch (IOException ioe) {
            ex = ioe;
            throw ioe;
        } catch (Exception e) {
            log.error(String.format("%s Exception: totalWritten so far %d to %s",
                    context.getId(), recordCount, context.getDataSource()), e);
            ex = new IOException(e.getMessage(), e);
            throw ex;
        } finally {
            try {
                bridge.endIteration();
            } catch (IOException ioe) {
                ex = (ex == null) ? ioe : ex;
            } catch (Exception e) {
                ex = (ex == null) ? new IOException(e.getMessage(), e) : ex;
            }
        }

        // Report any errors we might have encountered
        if (ex != null) throw ex;

        return OperationStats.builder().operation("write").recordCount(recordCount).build();
    }
}
