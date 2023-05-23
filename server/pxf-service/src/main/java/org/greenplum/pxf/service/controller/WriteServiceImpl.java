package org.greenplum.pxf.service.controller;

import com.google.common.io.CountingInputStream;
import lombok.extern.slf4j.Slf4j;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.service.MetricsReporter;
import org.greenplum.pxf.service.bridge.Bridge;
import org.greenplum.pxf.service.bridge.BridgeFactory;
import org.greenplum.pxf.service.security.SecurityService;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.InputStream;

/**
 * Implementation of the WriteService.
 */
@Service
@Slf4j
public class WriteServiceImpl extends BaseServiceImpl<OperationStats> implements WriteService {

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
    public String writeData(RequestContext context, InputStream inputStream) throws Exception {
        OperationStats stats = processData(context, () -> readStream(context, inputStream));

        String censuredPath = Utilities.maskNonPrintables(context.getDataSource());
        String returnMsg = String.format("wrote %d records to %s", stats.getRecordCount(), censuredPath);
        log.debug(returnMsg);

        return returnMsg;
    }

    /**
     * Reads the input stream, iteratively submits data from the stream to created bridge.
     *
     * @param context     request context
     * @param inputStream input stream
     * @return operation statistics
     */
    private OperationResult readStream(RequestContext context, InputStream inputStream) {
        Bridge bridge = getBridge(context);

        OperationStats operationStats = new OperationStats(OperationStats.Operation.WRITE, metricsReporter, context);
        OperationResult operationResult = new OperationResult();

        // dataStream (and inputStream as the result) will close automatically at the end of the try block
        CountingInputStream countingInputStream = new CountingInputStream(inputStream);
        try (DataInputStream dataStream = new DataInputStream(countingInputStream)) {
            // open the output file, returns true or throws an error
            bridge.beginIteration();
            while (bridge.setNext(dataStream)) {
                operationStats.reportCompletedRecord(countingInputStream.getCount());
            }
        } catch (Exception e) {
            operationResult.setException(e);
        } finally {
            try {
                bridge.endIteration();
            } catch (Exception e) {
                if (operationResult.getException() == null) {
                    operationResult.setException(e);
                }
            }

            // in the case where we fail to report a record due to an exception,
            // report the number of bytes that we were able to read before failure
            operationStats.setByteCount(countingInputStream.getCount());
            operationStats.flushStats();
            operationResult.setStats(operationStats);
        }

        return operationResult;
    }
}
