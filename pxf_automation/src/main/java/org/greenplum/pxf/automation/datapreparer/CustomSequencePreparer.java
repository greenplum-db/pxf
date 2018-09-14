package org.greenplum.pxf.automation.datapreparer;

import java.lang.reflect.Constructor;
import java.util.ArrayList;

import org.greenplum.pxf.automation.fileformats.IDataPreparer;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/**
 * Data Preparer for Sequence using following fields created by CustomWritable.
 * The data is created by CustomWritable, and consists of the following fields:
 * string, int array (size 2), int, int, string array (size 5), string, double
 * array (size 2), double, float array (size 2), float, long array (size 2),
 * long, boolean array (size 2), boolean, short array (size 4), short, byte
 * array.
 */
public class CustomSequencePreparer implements IDataPreparer {

    @Override
    public Object[] prepareData(int rows, Table dataTable) throws Exception {

        Object[] cwArr = new Object[rows];

        for (int i = 0; i < cwArr.length; i++) {

            ArrayList<String> row = new ArrayList<String>();

            int num1 = i;

            String tms = "1919-06-28 23:59:59.2233";

            // full package name
            Class<?> c = Class.forName("org.greenplum.pxf.automation.dataschema.CustomWritable");

            Constructor<?> constructor = c.getConstructor(String.class,
                    int.class, int.class, int.class);

            constructor.setAccessible(true);

            row.add(tms.toString());

            cwArr[i] = constructor.newInstance(tms, num1, 10 * num1, 20 * num1);

            if (i > 0) {
                /**
                 * Read fields and store in data table
                 */

                int[] num = ((int[]) cwArr[i].getClass().getField("num").get(
                        cwArr[i]));

                for (int j = 0; j < num.length; j++) {
                    row.add(String.valueOf(num[j]));
                }

                int int1 = ((Integer) cwArr[i].getClass().getField("int1").get(
                        cwArr[i]));

                row.add(String.valueOf(int1));

                int int2 = ((Integer) cwArr[i].getClass().getField("int2").get(
                        cwArr[i]));

                row.add(String.valueOf(int2));

                String[] strings = ((String[]) cwArr[i].getClass().getField(
                        "strings").get(cwArr[i]));

                for (int j = 0; j < strings.length; j++) {
                    row.add(strings[j]);
                }

                String st1 = ((String) cwArr[i].getClass().getField("st1").get(
                        cwArr[i]));

                row.add(st1);

                double[] dubs = ((double[]) cwArr[i].getClass().getField("dubs").get(
                        cwArr[i]));

                for (int j = 0; j < dubs.length; j++) {
                    row.add(String.valueOf(dubs[j]));
                }

                double db = ((Double) cwArr[i].getClass().getField("db").get(
                        cwArr[i]));

                row.add(String.valueOf(db));

                float[] fts = ((float[]) cwArr[i].getClass().getField("fts").get(
                        cwArr[i]));

                for (int j = 0; j < fts.length; j++) {
                    row.add(String.valueOf(fts[j]));
                }

                float ft = ((Float) cwArr[i].getClass().getField("ft").get(
                        cwArr[i]));

                row.add(String.valueOf(ft));

                long[] lngs = ((long[]) cwArr[i].getClass().getField("lngs").get(
                        cwArr[i]));

                for (int j = 0; j < lngs.length; j++) {
                    row.add(String.valueOf(lngs[j]));
                }

                long lng = ((Long) cwArr[i].getClass().getField("lng").get(
                        cwArr[i]));

                row.add(String.valueOf(lng));

                boolean[] bools = ((boolean[]) cwArr[i].getClass().getField(
                        "bools").get(cwArr[i]));
                for (int j = 0; j < bools.length; j++) {
                    row.add(String.valueOf(bools[j]));
                }

                boolean bool = ((Boolean) cwArr[i].getClass().getField("bool").get(
                        cwArr[i]));
                row.add(String.valueOf(bool));

                short[] shrts = ((short[]) cwArr[i].getClass().getField("shrts").get(
                        cwArr[i]));

                for (int j = 0; j < shrts.length; j++) {
                    row.add(String.valueOf(shrts[j]));
                }

                long shrt = ((Short) cwArr[i].getClass().getField("shrt").get(
                        cwArr[i]));

                row.add(String.valueOf(shrt));

                byte[] bts = ((byte[]) cwArr[i].getClass().getField("bts").get(
                        cwArr[i]));

                row.add(new String(bts));

                dataTable.addRow(row);
            }
        }

        return cwArr;
    }

}
