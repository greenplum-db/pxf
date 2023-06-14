package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.error.UnsupportedTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DecimalOverflowOptionTest {
    private static final String PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME = "pxf.orc.write.decimal.overflow";
    private static final String PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME = "pxf.parquet.write.decimal.overflow";
    private Configuration configuration;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        configuration.clear();
    }

    @Test
    public void testDefaultDecimalOverflowOptionWhenPropertyNotSet() {
        DecimalOverflowOption orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND, orcDecimalOverflowOption);

        DecimalOverflowOption parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND, parquetDecimalOverflowOption);
    }

    @Test
    public void testDefaultDecimalOverflowOptionError() {
        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "error");
        DecimalOverflowOption orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR, orcDecimalOverflowOption);

        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "ERROR");
        orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR, orcDecimalOverflowOption);

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "error");
        DecimalOverflowOption parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR, parquetDecimalOverflowOption);

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "ERROR");
        parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_ERROR, parquetDecimalOverflowOption);
    }

    @Test
    public void testDefaultDecimalOverflowOptionRound() {
        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "round");
        DecimalOverflowOption orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND, orcDecimalOverflowOption);

        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "ROUND");
        orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND, orcDecimalOverflowOption);

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "round");
        DecimalOverflowOption parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND, parquetDecimalOverflowOption);

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "ROUND");
        parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_ROUND, parquetDecimalOverflowOption);
    }

    @Test
    public void testDefaultDecimalOverflowOptionIgnore() {
        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "ignore");
        DecimalOverflowOption orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING, orcDecimalOverflowOption);

        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "IGNORE");
        orcDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false);
        assertDecimalOverflowOption(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING, orcDecimalOverflowOption);

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "ignore");
        DecimalOverflowOption parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE, parquetDecimalOverflowOption);

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, "IGNORE");
        parquetDecimalOverflowOption = DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, true);
        assertEquals(DecimalOverflowOption.DECIMAL_OVERFLOW_IGNORE, parquetDecimalOverflowOption);
    }

    @Test
    public void testDefaultDecimalOverflowOptionInvalid() {
        String invalidOption = "123";
        configuration.set(PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, invalidOption);
        Exception orcException = assertThrows(UnsupportedTypeException.class, () -> DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false));
        assertEquals(String.format("Invalid configuration value %s for configuration property %s. " +
                "Valid values are 'error', 'round', and 'ignore'.", invalidOption, PXF_ORC_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME), orcException.getMessage());

        configuration.set(PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, invalidOption);
        Exception parquetException = assertThrows(UnsupportedTypeException.class, () -> DecimalOverflowOption.parseDecimalOverflowOption(configuration, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME, false));
        assertEquals(String.format("Invalid configuration value %s for configuration property %s. " +
                "Valid values are 'error', 'round', and 'ignore'.", invalidOption, PXF_PARQUET_WRITE_DECIMAL_OVERFLOW_PROPERTY_NAME), parquetException.getMessage());
    }

    private void assertDecimalOverflowOption(DecimalOverflowOption expectedOption, DecimalOverflowOption option) {
        assertEquals(expectedOption.getDecimalOverflowOption(), option.getDecimalOverflowOption());
        assertEquals(expectedOption.wasEnforcedPrecisionAndScale(), option.wasEnforcedPrecisionAndScale());
        switch (expectedOption) {
            case DECIMAL_OVERFLOW_ERROR:
                assertTrue(option.isOptionError());
                assertFalse(option.isOptionRound());
                assertFalse(option.isOptionIgnore());
                break;
            case DECIMAL_OVERFLOW_ROUND:
                assertTrue(option.isOptionRound());
                assertFalse(option.isOptionError());
                assertFalse(option.isOptionIgnore());
                break;
            case DECIMAL_OVERFLOW_IGNORE:
            case DECIMAL_OVERFLOW_IGNORE_WITHOUT_ENFORCING:
                assertTrue(option.isOptionIgnore());
                assertFalse(option.isOptionError());
                assertFalse(option.isOptionRound());
                break;
            default:
                throw new UnsupportedTypeException(String.format("Unsupported decimal overflow option %s.", expectedOption.getDecimalOverflowOption()));
        }
    }
}
