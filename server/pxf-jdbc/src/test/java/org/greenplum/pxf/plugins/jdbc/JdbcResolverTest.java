package org.greenplum.pxf.plugins.jdbc;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.security.SecureLogin;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class JdbcResolverTest {
    @Mock
    private OneRow row;
    @Mock
    private ResultSet result;
    @Mock
    private ConnectionManager mockConnectionManager;
    @Mock
    private SecureLogin mockSecureLogin;
    @Mock
    private PreparedStatement mockStatement;
    RequestContext context = new RequestContext();
    List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
    List<OneField> oneFieldList = new ArrayList<>();
    private JdbcResolver resolver;
    private boolean isDateWideRange;

    @BeforeEach
    void setup() {
        resolver = new JdbcResolver(mockConnectionManager, mockSecureLogin);
    }

    @Test
    void getFieldDateWithWideRangeTest() throws SQLException {
        isDateWideRange = true;
        LocalDate localDate = LocalDate.of(1977, 12, 11);
        OneField oneField = getOneField(localDate, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof String);
        assertEquals("1977-12-11 AD", oneField.val);
    }

    @Test
    void getFieldDateWithoutWideRangeTest() throws SQLException {
        isDateWideRange = false;
        Date date = Date.valueOf("1977-12-11");
        OneField oneField = getOneField(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof String);
        assertEquals("1977-12-11", oneField.val);
    }

    @Test
    void getFieldDateNullWithWideRangeTest() throws SQLException {
        isDateWideRange = true;
        OneField oneField = getOneField(null, DataType.DATE.getOID(), "date");
        assertNull(oneField.val);
    }

    @Test
    void getFieldDateWithWideRangeWithLeadingZeroTest() throws SQLException {
        isDateWideRange = true;
        LocalDate localDate = LocalDate.of(3, 5, 4);
        OneField oneField = getOneField(localDate, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof String);
        assertEquals("0003-05-04 AD", oneField.val);
    }

    @Test
    void getFieldDateWithMoreThan4digitsInYearTest() throws SQLException {
        isDateWideRange = true;
        LocalDate localDate = LocalDate.of(+12345678, 12, 11);
        OneField oneField = getOneField(localDate, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof String);
        assertEquals("12345678-12-11 AD", oneField.val);
    }

    @Test
    void getFieldDateWithEraWithMoreThan4digitsInYearTest() throws SQLException {
        isDateWideRange = true;
        LocalDate localDate = LocalDate.of(-1234567, 6, 1);
        OneField oneField = getOneField(localDate, DataType.DATE.getOID(), "date");
        // The year -1234567 is transferred to 1234568 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
        assertTrue(oneField.val instanceof String);
        assertEquals("1234568-06-01 BC", oneField.val);
    }

    @Test
    void getFieldDateWithEraTest() throws SQLException {
        isDateWideRange = true;
        LocalDate localDate = LocalDate.of(-1234, 6, 1);
        OneField oneField = getOneField(localDate, DataType.DATE.getOID(), "date");
        // The year -1234 is transferred to 1235 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
        assertTrue(oneField.val instanceof String);
        assertEquals("1235-06-01 BC", oneField.val);
    }

    @Test
    void getFieldDateTimeWithWideRangeTest() throws SQLException {
        isDateWideRange = true;
        LocalDateTime localDateTime = LocalDateTime.parse("1977-12-11T11:15:30.1234");
        OneField oneField = getOneField(localDateTime, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof String);
        assertEquals("1977-12-11 11:15:30.1234 AD", oneField.val);
    }

    @Test
    void getFieldDateTimeWithoutWideRangeTest() throws SQLException {
        isDateWideRange = false;
        Timestamp timestamp = Timestamp.valueOf("1977-12-11 11:15:30.1234");
        OneField oneField = getOneField(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof String);
        assertEquals("1977-12-11 11:15:30.1234", oneField.val);
    }

    @Test
    void getFieldDateNullTimeWithWideRangeTest() throws SQLException {
        isDateWideRange = true;
        OneField oneField = getOneField(null, DataType.TIMESTAMP.getOID(), "timestamp");
        assertNull(oneField.val);
    }

    @Test
    void getFieldDateTimeWithWideRangeWithLeadingZeroTest() throws SQLException {
        isDateWideRange = true;
        LocalDateTime localDateTime = LocalDateTime.parse("0003-01-02T04:05:06.0000015");
        OneField oneField = getOneField(localDateTime, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof String);
        assertEquals("0003-01-02 04:05:06.0000015 AD", oneField.val);
    }

    @Test
    void getFieldDateTimeWithMoreThan4digitsInYearTest() throws SQLException {
        isDateWideRange = true;
        LocalDateTime localDateTime = LocalDateTime.parse("+9876543-12-11T11:15:30.1234");
        OneField oneField = getOneField(localDateTime, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof String);
        assertEquals("9876543-12-11 11:15:30.1234 AD", oneField.val);
    }

    @Test
    void getFieldDateTimeWithEraTest() throws SQLException {
        isDateWideRange = true;
        LocalDateTime localDateTime = LocalDateTime.parse("-3456-12-11T11:15:30");
        OneField oneField = getOneField(localDateTime, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof String);
        // The year -3456 is transferred to 3457 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
        assertEquals("3457-12-11 11:15:30 BC", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeTest() throws SQLException {
        isDateWideRange = false;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("1977-12-11T10:15:30.1234+05:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("1977-12-11 10:15:30.1234+05", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeWithWideRangeTest() throws SQLException {
        isDateWideRange = true;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("1977-12-11T10:15:30.1234+05:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("1977-12-11 10:15:30.1234+05:00 AD", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeNullWithWideRangeTest() throws SQLException {
        isDateWideRange = true;
        OneField oneField = getOneField(null, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertNull(oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeWithLeadingZeroTest() throws SQLException {
        isDateWideRange = false;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("0003-01-02T04:05:06.000001+03:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("0003-01-02 04:05:06.000001+03", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeWithWideRangeWithLeadingZeroTest() throws SQLException {
        isDateWideRange = true;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("0003-01-02T04:05:06.0000015+03:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("0003-01-02 04:05:06.0000015+03:00 AD", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeWithMoreThan4digitsInYearTest() throws SQLException {
        isDateWideRange = true;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("+9876543-12-11T11:15:30.1234-03:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("9876543-12-11 11:15:30.1234-03:00 AD", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeWithEraTest() throws SQLException {
        isDateWideRange = true;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("-3456-12-11T11:15:30+02:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        // The year -3456 is transferred to 3457 BC: https://en.wikipedia.org/wiki/Astronomical_year_numbering
        assertEquals("3457-12-11 11:15:30+02:00 BC", oneField.val);
    }

    @Test
    void getFieldOffsetDateTime() throws SQLException {
        isDateWideRange = false;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("1234-11-22T11:15:30+02:00");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("1234-11-22 11:15:30+02", oneField.val);
    }

    @Test
    void getFieldOffsetDateTimeWithHalfHourOffset() throws SQLException {
        isDateWideRange = false;
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("1234-11-22T11:15:30+02:30");
        OneField oneField = getOneField(offsetDateTime, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof String);
        assertEquals("1234-11-22 11:15:30+02:30", oneField.val);
    }

    @Test
    void getFieldUUIDTest() throws SQLException {
        UUID uuid = UUID.fromString("decafbad-0000-0000-0000-000000000000");
        when(row.getData()).thenReturn(result);
        when(result.getObject("uuid_col", java.util.UUID.class)).thenReturn(uuid);
        columnDescriptors.add(new ColumnDescriptor("uuid_col", DataType.UUID.getOID(), 1, DataType.UUID.name(), null));
        context.setTupleDescription(columnDescriptors);
        resolver.columns = context.getTupleDescription();

        List<OneField> oneFields = resolver.getFields(row);
        assertEquals(1, oneFields.size());

        OneField oneField = oneFields.get(0);
        assertEquals(uuid, oneField.val);
    }

    @Test
    void setFieldDateWithWideRangeTest() throws ParseException {
        isDateWideRange = true;
        LocalDate expectedLocalDate = LocalDate.of(1977, 12, 11);
        String date = "1977-12-11";
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof LocalDate);
        assertEquals(expectedLocalDate, oneField.val);
    }

    @Test
    void setFieldDateWithoutWideRangeTest() throws ParseException {
        isDateWideRange = false;
        String date = "1977-12-11";
        Date expectedDate = Date.valueOf(date);
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof Date);
        assertEquals(expectedDate, oneField.val);
    }

    @Test
    void setFieldDateWithWideRangeWithLeadingZeroTest() throws ParseException {
        isDateWideRange = true;
        LocalDate expectedLocalDate = LocalDate.of(3, 5, 4);
        String date = "0003-05-04";
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof LocalDate);
        assertEquals(expectedLocalDate, oneField.val);
    }

    @Test
    void setFieldDateWithoutWideRangeWithLeadingZeroTest() throws ParseException {
        isDateWideRange = false;
        String date = "0003-5-4";
        Date expectedDate = Date.valueOf(date);
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof Date);
        assertEquals(expectedDate, oneField.val);
    }

    @Test
    void setFieldDateWithoutWideRangeWithoutLeadingZeroTest() {
        isDateWideRange = false;
        String date = "3-5-4";
        assertThrows(IllegalArgumentException.class, () -> setFields(date, DataType.DATE.getOID(), "date"));
    }

    @Test
    void setFieldDateWithMoreThan4digitsInYearTest() throws ParseException {
        isDateWideRange = true;
        LocalDate expectedLocalDate = LocalDate.of(+12345678, 12, 11);
        String date = "12345678-12-11";
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof LocalDate);
        assertEquals(expectedLocalDate, oneField.val);
    }

    @Test
    void setFieldDateWithMoreThan4digitsInYearWithoutWideRangeTest() throws ParseException {
        isDateWideRange = false;
        String date = "12345678-12-11";
        assertThrows(IllegalArgumentException.class, () -> setFields(date, DataType.DATE.getOID(), "date"));
    }

    @Test
    void setFieldDateWithEraWithMoreThan4digitsInYearTest() throws ParseException {
        isDateWideRange = true;
        LocalDate expectedLocalDate = LocalDate.of(-1234567, 6, 1);
        String date = "1234568-06-01 BC";
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof LocalDate);
        assertEquals(expectedLocalDate, oneField.val);
    }

    @Test
    void setFieldDateWithEraTest() throws ParseException {
        isDateWideRange = true;
        LocalDate expectedLocalDate = LocalDate.of(-1234, 11, 1);
        String date = "1235-11-01 BC";
        OneField oneField = setFields(date, DataType.DATE.getOID(), "date");
        assertTrue(oneField.val instanceof LocalDate);
        assertEquals(expectedLocalDate, oneField.val);
    }

    @Test
    void setFieldDateWithEraWithoutWideRangeTest() {
        isDateWideRange = false;
        String date = "1235-11-01 BC";
        assertThrows(IllegalArgumentException.class, () -> setFields(date, DataType.DATE.getOID(), "date"));
    }

    @Test
    void setFieldDateTimeWithWideRangeTest() throws ParseException {
        isDateWideRange = true;
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(1977, 12, 11, 15, 12, 11, 123456789);
        String timestamp = "1977-12-11 15:12:11.123456789";
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof LocalDateTime);
        assertEquals(expectedLocalDateTime, oneField.val);
    }

    @Test
    void setFieldDateTimeWithoutWideRangeTest() throws ParseException {
        isDateWideRange = false;
        String timestamp = "1977-12-11 15:12:11.123456789";
        Timestamp expectedDateTime = Timestamp.valueOf(timestamp);
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof Timestamp);
        assertEquals(expectedDateTime, oneField.val);
    }

    @Test
    void setFieldDateTimeWithWideRangeWithLeadingZeroTest() throws ParseException {
        isDateWideRange = true;
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(3, 5, 4, 1, 2, 1);
        String timestamp = "0003-05-04 01:02:01";
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof LocalDateTime);
        assertEquals(expectedLocalDateTime, oneField.val);
    }

    @Test
    void setFieldDateTimeWithoutWideRangeWithLeadingZeroTest() throws ParseException {
        isDateWideRange = false;
        String timestamp = "0003-05-04 01:02:01.23";
        Timestamp expectedTimestamp = Timestamp.valueOf(timestamp);
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof Timestamp);
        assertEquals(expectedTimestamp, oneField.val);
    }

    @Test
    void setFieldDateTimeWithMoreThan4digitsInYearTest() throws ParseException {
        isDateWideRange = true;
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(+12345678, 12, 11, 15, 35);
        String timestamp = "12345678-12-11 15:35 AD";
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof LocalDateTime);
        assertEquals(expectedLocalDateTime, oneField.val);
    }

    @Test
    void setFieldDateTimeWithMoreThan4digitsInYearWithoutWideRangeTest() {
        isDateWideRange = false;
        String timestamp = "12345678-12-11 15:35 AD";
        assertThrows(IllegalArgumentException.class, () -> setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp"));
    }

    @Test
    void setFieldDateTimeWithEraWithMoreThan4digitsInYearTest() throws ParseException {
        isDateWideRange = true;
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(-1234567, 6, 1, 19, 56, 43, 12);
        String timestamp = "1234568-06-01 19:56:43.000000012 BC";
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof LocalDateTime);
        assertEquals(expectedLocalDateTime, oneField.val);
    }

    @Test
    void setFieldDateTimeWithEraTest() throws ParseException {
        isDateWideRange = true;
        LocalDateTime expectedLocalDateTime = LocalDateTime.of(-1234, 11, 1, 16, 20);
        String timestamp = "1235-11-01 16:20 BC";
        OneField oneField = setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp");
        assertTrue(oneField.val instanceof LocalDateTime);
        assertEquals(expectedLocalDateTime, oneField.val);
    }

    @Test
    void setFieldDateTimeWithEraWithoutWideRangeTest() throws ParseException {
        isDateWideRange = false;
        String timestamp = "1235-11-01 16:20 BC";
        assertThrows(IllegalArgumentException.class, () -> setFields(timestamp, DataType.TIMESTAMP.getOID(), "timestamp"));
    }

    @Test
    void setFieldOffsetDateTimeWithoutWideRangeTest() throws ParseException {
        isDateWideRange = false;
        OffsetDateTime expectedOffsetDateTime = OffsetDateTime.of(1234, 1, 23, 11, 22, 33, 440000000, ZoneOffset.of("+12:34"));
        String timestamptz = "1234-01-23 11:22:33.44+12:34";
        OneField oneField = setFields(timestamptz, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof OffsetDateTime);
        assertEquals(expectedOffsetDateTime, oneField.val);
    }

    @Test
    void setFieldOffsetDateTimeWithWideRangeTest() throws ParseException {
        isDateWideRange = true;
        OffsetDateTime expectedOffsetDateTime = OffsetDateTime.of(12345, 1, 23, 11, 22, 33, 440000000, ZoneOffset.of("+12:34"));
        String timestamptz = "12345-01-23 11:22:33.44+12:34";
        OneField oneField = setFields(timestamptz, DataType.TIMESTAMP_WITH_TIME_ZONE.getOID(), "timestamptz");
        assertTrue(oneField.val instanceof OffsetDateTime);
        assertEquals(expectedOffsetDateTime, oneField.val);
    }

    @Test
    void setFieldUUIDTest() throws ParseException {
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), "decafbad-0000-0000-0000-000000000000"));
        columnDescriptors.add(new ColumnDescriptor("uuid_col", DataType.UUID.getOID(), 1, DataType.UUID.name(), null));
        context.setTupleDescription(columnDescriptors);
        resolver.columns = context.getTupleDescription();

        OneRow oneRow = resolver.setFields(oneFieldList);
        @SuppressWarnings("unchecked")
        List<OneField> oneFields = (List<OneField>) oneRow.getData();
        assertEquals(1, oneFields.size());

        OneField oneField = oneFields.get(0);
        assertEquals(UUID.fromString("decafbad-0000-0000-0000-000000000000"), oneField.val);
    }

    @Test
    void decodeOneRowToPreparedStatement_UUIDTest() throws SQLException, IOException {
        oneFieldList.add(new OneField(DataType.UUID.getOID(), UUID.fromString("decafbad-0000-0000-0000-000000000000")));
        when(row.getData()).thenReturn(oneFieldList);

        JdbcResolver.decodeOneRowToPreparedStatement(row, mockStatement);

        verify(mockStatement).setObject(1, UUID.fromString("decafbad-0000-0000-0000-000000000000"));
        verifyNoMoreInteractions(mockStatement);
    }

    private OneField getOneField(Object date, int dataTypeOid, String typeName) throws SQLException {
        when(row.getData()).thenReturn(result);
        if (date instanceof LocalDate) {
            when(result.getObject("birth_date", LocalDate.class)).thenReturn((LocalDate) date);
        } else if (date instanceof Date) {
            when(result.getDate("birth_date")).thenReturn((Date) date);
        } else if (date instanceof LocalDateTime) {
            when(result.getObject("birth_date", LocalDateTime.class)).thenReturn((LocalDateTime) date);
        } else if (date instanceof Timestamp) {
            when(result.getTimestamp("birth_date")).thenReturn((Timestamp) date);
        } else if (date instanceof OffsetDateTime) {
            when(result.getObject("birth_date", OffsetDateTime.class)).thenReturn((OffsetDateTime) date);
        }
        columnDescriptors.add(new ColumnDescriptor("birth_date", dataTypeOid, 1, typeName, null));
        context.setTupleDescription(columnDescriptors);
        resolver.columns = context.getTupleDescription();
        resolver.isDateWideRange = isDateWideRange;
        List<OneField> oneFields = resolver.getFields(row);
        return oneFields.get(0);
    }

    private OneField setFields(String date, int dataTypeOid, String typeName) throws ParseException {
        oneFieldList.add(new OneField(DataType.TEXT.getOID(), date));
        columnDescriptors.add(new ColumnDescriptor("birth_date", dataTypeOid, 0, typeName, null));
        context.setTupleDescription(columnDescriptors);
        resolver.columns = context.getTupleDescription();
        resolver.isDateWideRange = isDateWideRange;
        OneRow oneRow = resolver.setFields(oneFieldList);
        return (OneField) ((List<?>) oneRow.getData()).get(0);
    }
}
