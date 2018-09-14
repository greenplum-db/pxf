package org.greenplum.pxf.automation.datapreparer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Data Preparer for Text using following fields: "text", "text", "text",
 * "timestamp", "int", "int", "int", "int", "int", "int", "int", "text", "text",
 * "text", "timestamp", "int", "int", "int", "int", "int", "int", "int"
 */
public class CustomTextPreparer implements IDataPreparer {

    @Override
    public Object[] prepareData(int rows, Table dataTable) throws Exception {

        Object[] data = new Object[rows];

        // fill data and dataTable with data according to given rows
        for (int i = 0, num1 = 1; i < rows; i++, num1++) {

            ArrayList<String> row = new ArrayList<String>();

            // create calendar and set timestamp as milliseconds
            Calendar calendar = new GregorianCalendar();
            /*
             * Postgres max timestamp is 294276 year, which is
             * 92243*100000000000L in milliseconds. In order to avoid out of
             * range error when reading the data, we have to make sure the dates
             * are within the allowed range.
             */
            calendar.setTimeInMillis((num1 % 92243) * 100000000000L);

            /*
             * create SimpleDateFormat with "yyyy-MM-dd hh:mm:ss" format and set
             * a constant time zone to prevent result changes in different time
             * zones
             */
            SimpleDateFormat dateFormat = new SimpleDateFormat(
                    "yyyy-MM-dd hh:mm:ss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            // get timestamp value from SimpleDateFormat
            String timeStampValue = dateFormat.format(calendar.getTime());

            row.add("s_" + num1);
            row.add("s_" + num1 * 10);
            row.add("s_" + num1 * 100);
            row.add(timeStampValue);
            row.add(String.valueOf(num1));
            row.add(String.valueOf(num1 * 10));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add("s_" + num1);
            row.add("s_" + num1 * 10);
            row.add("s_" + num1 * 100);
            row.add(timeStampValue);
            row.add(String.valueOf(num1));
            row.add(String.valueOf(num1 * 10));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));
            row.add(String.valueOf(num1 * 100));

            dataTable.addRow(row);
            data[i] = row;
        }

        return data;
    }
}
