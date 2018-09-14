package org.greenplum.pxf.automation.utils.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FileUtil;

import org.greenplum.pxf.automation.components.hdfs.Hdfs;
import org.greenplum.pxf.automation.structures.data.DataPattern;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/*
 * Utility Class that is used to generate data and load it to HDFS.
 */
public class DataUtils {

    private static final long MB_UNIT = 1024 * 1024;
    private static final String dataTempFolder = "dataTempFolder";

    /*
     * Create the data using meta data information and load it to HDFS
     *
     * @param MetaTableData
     *
     * @throws Exception
     *
     * @return number of written lines
     */
    public static long generateAndLoadData(Table table, final Hdfs hdfs)
            throws Exception {

        DataPattern dataPattern = table.getDataPattern();
        long numberOfLines = 0;

        // Create local file and fill it with the generated data
        String textFilePath = hdfs.getWorkingDirectory() + "/"
                + table.getName();
        String localDataFile = dataTempFolder + "/" + table.getName();

        File file = new File(localDataFile);
        file.delete();

        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(
                file, true)));

        if (table.getDataPattern().getRecordValues() != null) {
            // Generate fixed prepared values (data) from set record values

            String recordValues = dataPattern.getRecordValues();
            numberOfLines = dataPattern.getDataSizeInMegaBytes() * MB_UNIT
                    / (recordValues.length() + 1); // +1 for newline
            for (long l = 0; l < numberOfLines; l++) {
                out.println(recordValues);
            }

            // Will be used to verify the query result
            table.setNumberOfLines(numberOfLines);

        } else { // Generate dynamic values (data) using meta data info

            RecordGenerator recordGenerator = new RecordGenerator(
                    dataPattern.getColumnMaxSize(),
                    dataPattern.getColumnsTypeList(),
                    dataPattern.getColumnDelimiter(),
                    dataPattern.isRandomValues());

            long totalByteSize = dataPattern.getDataSizeInMegaBytes() * MB_UNIT;
            long writtenBytes = 0;
            numberOfLines = 0;
            for (long l = 0; l < totalByteSize; l += writtenBytes) {
                String recordValues = recordGenerator.nextRecord();
                writtenBytes = recordValues.length() + 1; // +1 for newline
                numberOfLines++;
                out.println(recordValues);
            }
            // Will be used to verify the query result
            table.setNumberOfLines(numberOfLines);
        }

        out.close();

        // Copy data from local file to HDFS
        hdfs.copyFromLocal(localDataFile, textFilePath);

        // Remove local file
        FileUtil.fullyDelete(new File(localDataFile));

        return numberOfLines;
    }
}
