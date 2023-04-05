package org.greenplum.pxf.plugins.jdbc;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SpringContext;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
class JdbcResolverTest {
    @Mock
    private OneRow row;
    @Mock
    private ResultSet result;
    RequestContext context = new RequestContext();
    List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
    List<OneField> oneFieldList = new ArrayList<>();
    private JdbcResolver resolver;

    @Test
    void getFieldDateWithWideRangeTest() throws SQLException {
        LocalDate localDate = LocalDate.of(1977, 12, 11);
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDate.class)).thenReturn(localDate);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("1977-12-11 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateNullWithWideRangeTest() throws SQLException {
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDate.class)).thenReturn(null);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertNull(oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateWithWideRangeWithLeadingZeroTest() throws SQLException {
        LocalDate localDate = LocalDate.of(3, 5, 4);
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDate.class)).thenReturn(localDate);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("0003-05-04 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateWithMoreThan4digitsInYearTest() throws SQLException {
        LocalDate localDate = LocalDate.of(+12345678, 12, 11);
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDate.class)).thenReturn(localDate);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("12345678-12-11 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateWithEraWithMoreThan4digitsInYearTest() throws SQLException {
        LocalDate localDate = LocalDate.of(-1234567, 6, 1);
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDate.class)).thenReturn(localDate);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            // The year -1234567 is transferred to 1234568 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
            assertEquals("1234568-06-01 BC", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateWithEraTest() throws SQLException {
        LocalDate localDate = LocalDate.of(-1234, 6, 1);
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDate.class)).thenReturn(localDate);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            // The year -1234 is transferred to 1235 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
            assertEquals("1235-06-01 BC", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateTimeWithWideRangeTest() throws SQLException {
        LocalDateTime localDateTime = LocalDateTime.parse("1977-12-11T11:15:30.1234");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDateTime.class)).thenReturn(localDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("1977-12-11 11:15:30.1234 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateNullTimeWithWideRangeTest() throws SQLException {
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDateTime.class)).thenReturn(null);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertNull(oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateTimeWithWideRangeWithLeadingZeroTest() throws SQLException {
        LocalDateTime localDateTime = LocalDateTime.parse("0003-01-02T04:05:06.0000015");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDateTime.class)).thenReturn(localDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("0003-01-02 04:05:06.0000015 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateTimeWithMoreThan4digitsInYearTest() throws SQLException {
        LocalDateTime localDateTime = LocalDateTime.parse("+9876543-12-11T11:15:30.1234");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDateTime.class)).thenReturn(localDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("9876543-12-11 11:15:30.1234 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldDateTimeWithEraTest() throws SQLException {
        LocalDateTime localDateTime = LocalDateTime.parse("-3456-12-11T11:15:30");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", LocalDateTime.class)).thenReturn(localDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            // The year -3456 is transferred to 3457 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
            assertEquals("3457-12-11 11:15:30 BC", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldOffsetDateTimeWithWideRangeTest() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("1977-12-11T10:15:30.1234+05:00");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 1, "timestamptz", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", OffsetDateTime.class)).thenReturn(offsetDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("1977-12-11 10:15:30.1234+05:00 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldOffsetDateTimeNullWithWideRangeTest() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 1, "timestamptz", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", OffsetDateTime.class)).thenReturn(null);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertNull(oneFields.get(0).val);
        }
    }

    @Test
    void getFieldOffsetDateTimeWithWideRangeWithLeadingZeroTest() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("0003-01-02T04:05:06.0000015+03:00");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 1, "timestamptz", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", OffsetDateTime.class)).thenReturn(offsetDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("0003-01-02 04:05:06.0000015+03:00 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldOffsetDateTimeWithMoreThan4digitsInYearTest() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("+9876543-12-11T11:15:30.1234-03:00");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 1, "timestamptz", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", OffsetDateTime.class)).thenReturn(offsetDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            assertEquals("9876543-12-11 11:15:30.1234-03:00 AD", oneFields.get(0).val);
        }
    }

    @Test
    void getFieldOffsetDateTimeWithEraTest() throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("-3456-12-11T11:15:30+02:00");
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), 1, "timestamptz", null));
        context.setTupleDescription(columnDescriptors);
        when(row.getData()).thenReturn(result);
        when(result.getObject("birth_date", OffsetDateTime.class)).thenReturn(offsetDateTime);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            List<OneField> oneFields = resolver.getFields(row);
            // The year -3456 is transferred to 3457 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
            assertEquals("3457-12-11 11:15:30+02:00 BC", oneFields.get(0).val);
        }
    }

    @Test
    void setFieldDateWithWideRangeTest() throws ParseException {
        LocalDate expectedLocalDate = LocalDate.of(1977, 12, 11);
        String date = "1977-12-11";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDate);
            assertEquals(expectedLocalDate, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateWithWideRangeWithLeadingZeroTest() throws ParseException {
        LocalDate expectedLocalDate = LocalDate.of(3, 5, 4);
        String date = "0003-05-04";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDate);
            assertEquals(expectedLocalDate, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateWithMoreThan4digitsInYearTest() throws ParseException {
        LocalDate expectedLocalDate = LocalDate.of(+12345678, 12, 11);
        String date = "12345678-12-11";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDate);
            assertEquals(expectedLocalDate, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateWithEraWithMoreThan4digitsInYearTest() throws ParseException {
        LocalDate expectedLocalDate = LocalDate.of(-1234567, 6, 1);
        String date = "1234568-06-01 BC";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDate);
            assertEquals(expectedLocalDate, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateWithEraTest() throws ParseException {
        LocalDate expectedLocalDate = LocalDate.of(-1234, 11, 1);
        String date = "1235-11-01 BC";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.DATE.getOID(), 0, "date", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDate);
            assertEquals(expectedLocalDate, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateTimeWithWideRangeTest() throws ParseException {
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(1977, 12, 11, 15, 12, 11, 123456789);
        String date = "1977-12-11 15:12:11.123456789";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDateTime);
            assertEquals(expectedLocalDateTime, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateTimeWithWideRangeWithLeadingZeroTest() throws ParseException {
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(3, 5, 4, 1, 2, 1);
        String date = "0003-05-04 01:02:01";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDateTime);
            assertEquals(expectedLocalDateTime, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateTimeWithMoreThan4digitsInYearTest() throws ParseException {
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(+12345678, 12, 11, 15, 35);
        String date = "12345678-12-11 15:35 AD";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDateTime);
            assertEquals(expectedLocalDateTime, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateTimeWithEraWithMoreThan4digitsInYearTest() throws ParseException {
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(-1234567, 6, 1, 19, 56, 43, 12);
        String date = "1234568-06-01 19:56:43.000000012 BC";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDateTime);
            assertEquals(expectedLocalDateTime, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }

    @Test
    void setFieldDateTimeWithEraTest() throws ParseException {
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(-1234, 11, 1, 16, 20);
        String date = "1235-11-01 16:20 BC";
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        boolean isDateWideRange = true;
        columnDescriptors.add(new ColumnDescriptor("birth_date", DataType.TIMESTAMP.getOID(), 1, "timestamp", null));
        context.setTupleDescription(columnDescriptors);

        try (MockedStatic<SpringContext> springContextMockedStatic = mockStatic(SpringContext.class)) {
            springContextMockedStatic.when(() -> SpringContext.getBean(ConnectionManager.class)).thenReturn(mock(ConnectionManager.class));
            resolver = new JdbcResolver();
            resolver.columns = context.getTupleDescription();
            resolver.isDateWideRange = isDateWideRange;
            OneRow oneRow = resolver.setFields(oneFieldList);
            assertTrue(((OneField) ((List<?>) oneRow.getData()).get(0)).val instanceof LocalDateTime);
            assertEquals(expectedLocalDateTime, ((OneField) ((List<?>) oneRow.getData()).get(0)).val);
        }
    }
}
