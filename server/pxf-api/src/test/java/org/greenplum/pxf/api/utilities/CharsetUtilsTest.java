package org.greenplum.pxf.api.utilities;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CharsetUtilsTest {

    static Stream<String> unsupportedEncodings() {
        return Stream.of(
                "MULE_INTERNAL",
                "LATIN6",
                "LATIN8",
                "LATIN10",
                "GB18030",
                "SHIFT_JIS_2004",
                "EUC_JIS_2004"
        );
    }

    static Stream<Object[]> validEncodings() {
        return Stream.of(
                new Object[]{"SQL_ASCII", StandardCharsets.US_ASCII},
                new Object[]{"UNICODE", StandardCharsets.UTF_8},
                new Object[]{"UTF8", StandardCharsets.UTF_8},
                new Object[]{"LATIN1", Charset.forName("ISO8859_1")},
                new Object[]{"LATIN2", Charset.forName("ISO8859_2")},
                new Object[]{"LATIN3", Charset.forName("ISO8859_3")},
                new Object[]{"LATIN4", Charset.forName("ISO8859_4")},
                new Object[]{"ISO_8859_5", Charset.forName("ISO8859_5")},
                new Object[]{"ISO_8859_6", Charset.forName("ISO8859_6")},
                new Object[]{"ISO_8859_7", Charset.forName("ISO8859_7")},
                new Object[]{"ISO_8859_8", Charset.forName("ISO8859_8")},
                new Object[]{"LATIN5", Charset.forName("ISO8859_9")},
                new Object[]{"LATIN7", Charset.forName("ISO8859_13")},
                new Object[]{"LATIN9", Charset.forName("ISO8859_15_FDIS")},
                new Object[]{"EUC_JP", Charset.forName("EUC_JP")},
                new Object[]{"EUC_CN", Charset.forName("EUC_CN")},
                new Object[]{"EUC_KR", Charset.forName("EUC_KR")},
                new Object[]{"JOHAB", Charset.forName("Johab")},
                new Object[]{"EUC_TW", Charset.forName("EUC_TW")},
                new Object[]{"SJIS", Charset.forName("MS932")},
                new Object[]{"BIG5", Charset.forName("Big5")},
                new Object[]{"GBK", Charset.forName("GBK")},
                new Object[]{"UHC", Charset.forName("MS949")},
                new Object[]{"TCVN", Charset.forName("Cp1258")},
                new Object[]{"WIN1256", Charset.forName("Cp1256")},
                new Object[]{"WIN1250", Charset.forName("Cp1250")},
                new Object[]{"WIN1251", Charset.forName("Windows-1251")},
                new Object[]{"WIN1251", Charset.forName("Cp1251")},
                new Object[]{"WIN1252", Charset.forName("Windows-1252")},
                new Object[]{"WIN1253", Charset.forName("Windows-1253")},
                new Object[]{"WIN1254", Charset.forName("Windows-1254")},
                new Object[]{"WIN1255", Charset.forName("Windows-1255")},
                new Object[]{"WIN1257", Charset.forName("Windows-1257")},
                new Object[]{"WIN1258", Charset.forName("Windows-1258")},
                new Object[]{"WIN866", Charset.forName("Cp866")},
                new Object[]{"WIN874", Charset.forName("MS874")},
                new Object[]{"WIN", Charset.forName("Cp1251")},
                new Object[]{"ALT", Charset.forName("Cp866")},
                new Object[]{"KOI8", Charset.forName("KOI8_U")},
                new Object[]{"KOI8R", Charset.forName("KOI8_R")},
                new Object[]{"KOI8U", Charset.forName("KOI8_U")}
        );
    }

    CharsetUtils utils;

    @BeforeEach
    void setup() {
        utils = new CharsetUtils();
    }

    @MethodSource("unsupportedEncodings")
    @ParameterizedTest
    void testUnsupportedEncodings(String name) {
        assertThatThrownBy(() -> utils.forName(name))
                .isInstanceOf(IllegalCharsetNameException.class)
                .hasMessageContaining("The '" + name + "' encoding is not supported");
    }

    @Test
    void testInvalidEncoding() {
        assertThatThrownBy(() -> utils.forName("foo"))
                .isInstanceOf(IllegalCharsetNameException.class)
                .hasMessageContaining("The 'foo' encoding is not supported");
    }

    @MethodSource("validEncodings")
    @ParameterizedTest
    void testValidEncodings(String name, Charset expected) {
        assertThat(utils.forName(name)).isEqualTo(expected);
    }
}