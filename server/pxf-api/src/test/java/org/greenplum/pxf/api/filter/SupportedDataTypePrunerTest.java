package org.greenplum.pxf.api.filter;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SupportedDataTypePrunerTest {

    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private static final Integer[] TM = new Integer[]{}; // type modifiers

    private static final List<ColumnDescriptor> oneTextColumn = Arrays.asList(
            new ColumnDescriptor("c0", DataType.TEXT.getOID(), 0, "", TM)
    );

    private static final List<ColumnDescriptor> oneBooleanColumn = Arrays.asList(
            new ColumnDescriptor("c0", DataType.BOOLEAN.getOID(), 0, "", TM)
    );

    private static final List<ColumnDescriptor> twoColumns = Arrays.asList(
            new ColumnDescriptor("c0", DataType.TEXT.getOID(), 0, "", TM),
            new ColumnDescriptor("c1", DataType.INTEGER.getOID(), 1, "", TM)
    );

    private static final List<ColumnDescriptor> threeColumns = Arrays.asList(
            new ColumnDescriptor("c0", DataType.TEXT.getOID(), 0, "", TM),
            new ColumnDescriptor("c1", DataType.INTEGER.getOID(), 1, "", TM),
            new ColumnDescriptor("c2", DataType.FLOAT8.getOID(), 2, "", TM)
    );

    private static final List<ColumnDescriptor> fourColumns = Arrays.asList(
            new ColumnDescriptor("c0", DataType.TEXT.getOID(), 0, "", TM),
            new ColumnDescriptor("c1", DataType.INTEGER.getOID(), 1, "", TM),
            new ColumnDescriptor("c2", DataType.FLOAT8.getOID(), 2, "", TM),
            new ColumnDescriptor("c3", DataType.BPCHAR.getOID(), 3, "", TM)
    );

    private EnumSet<DataType> supportedTypes;

    // ---------- TEST SIMPLE OPERATORS ----------
    @Test
    public void test_simpleOperator_SupportedType() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("_0_ >= bar", "a0c25s3dbaro4", oneTextColumn, supportedTypes);
    }

    @Test
    public void test_simpleOperator_UnsupportedType() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro4", oneTextColumn, supportedTypes);
    }

    @Test
    public void test_simpleOperator_InvalidType() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        List<ColumnDescriptor> invalidColumn = Arrays.asList(
                new ColumnDescriptor("c0", 12345678, 0, "", TM)
        );
        helper("", "a0c25s3dbaro4", invalidColumn, supportedTypes);
    }

    // ---------- TEST SINGLE LEVEL FILTERING WITH LOGICAL OPERATORS ----------
    // --- when child got pruned from AND, the other one gets promoted
    // --- when child got pruned from OR , the other one gets removed as well

    // ---------- (P1 AND P2) ----------
    @Test
    public void test_AND_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("(_0_ = bar AND _1_ <> 999)", "a0c25s3dbaro5a1c23s3d999o6l0", twoColumns, supportedTypes);
    }

    @Test
    public void test_AND_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("_1_ <> 999", "a0c25s3dbaro5a1c23s3d999o6l0", twoColumns, supportedTypes);
    }

    @Test
    public void test_AND_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("_0_ = bar", "a0c25s3dbaro5a1c23s3d999o6l0", twoColumns, supportedTypes);
    }

    @Test
    public void test_AND_Prune_Both() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0", twoColumns, supportedTypes);
    }

    // ---------- (P1 OR P2) ----------
    @Test
    public void test_OR_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("(_0_ = bar OR _1_ <> 999)", "a0c25s3dbaro5a1c23s3d999o6l1", twoColumns, supportedTypes);
    }

    @Test
    public void test_OR_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1", twoColumns, supportedTypes);
    }

    @Test
    public void test_OR_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1", twoColumns, supportedTypes);
    }

    @Test
    public void test_OR_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1", twoColumns, supportedTypes);
    }

    // ---------- (P1 AND P2) AND P3 ----------
    @Test
    public void test_AND_AND_LeftDeep_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("((_0_ = bar AND _1_ <> 999) AND _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("(_1_ <> 999 AND _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("(_0_ = bar AND _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("(_0_ = bar AND _1_ <> 999)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_12() throws Exception {
        supportedTypes = EnumSet.of(DataType.FLOAT8);
        helper("_2_ < 123", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("_0_ = bar", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_13() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("_1_ <> 999", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_LeftDeep_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l0", threeColumns, supportedTypes);
    }

    // ---------- P1 AND (P2 AND P3) ----------
    @Test
    public void test_AND_AND_RightDeep_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("(_0_ = bar AND (_1_ <> 999 AND _2_ < 123))", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("(_1_ <> 999 AND _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("(_0_ = bar AND _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("(_0_ = bar AND _1_ <> 999)", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_12() throws Exception {
        supportedTypes = EnumSet.of(DataType.FLOAT8);
        helper("_2_ < 123", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("_0_ = bar", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_13() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("_1_ <> 999", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_RightDeep_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l0l0", threeColumns, supportedTypes);
    }

    // ---------- (P1 AND P2) OR P3 ----------
    @Test
    public void test_AND_OR_LeftDeep_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("((_0_ = bar AND _1_ <> 999) OR _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("(_1_ <> 999 OR _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("(_0_ = bar OR _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_12() throws Exception {
        supportedTypes = EnumSet.of(DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_13() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_LeftDeep_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    // ---------- P1 AND (P2 OR P3) ----------
    @Test
    public void test_AND_OR_RightDeep_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("(_0_ = bar AND (_1_ <> 999 OR _2_ < 123))", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("(_1_ <> 999 OR _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("_0_ = bar", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("_0_ = bar", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_12() throws Exception {
        supportedTypes = EnumSet.of(DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("_0_ = bar", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_13() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_OR_RightDeep_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l0", threeColumns, supportedTypes);
    }

    // ---------- (P1 OR P2) OR P3 ----------
    @Test
    public void test_OR_OR_LeftDeep_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("((_0_ = bar OR _1_ <> 999) OR _2_ < 123)", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_12() throws Exception {
        supportedTypes = EnumSet.of(DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_13() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_LeftDeep_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6l1a2c701s3d123o1l1", threeColumns, supportedTypes);
    }

    // ---------- P1 OR (P2 OR P3) ----------
    @Test
    public void test_OR_OR_RightDeep_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("(_0_ = bar OR (_1_ <> 999 OR _2_ < 123))", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_12() throws Exception {
        supportedTypes = EnumSet.of(DataType.FLOAT8);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_13() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    @Test
    public void test_OR_OR_RightDeep_Prune_All() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5a1c23s3d999o6a2c701s3d123o1l1l1", threeColumns, supportedTypes);
    }

    // ---------- Some more random combination of predicates ----------

    @Test
    public void test_AND_AND_OR_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8, DataType.BPCHAR);
        helper("((_0_ = bar AND _1_ <> 999) AND (_2_ < 123 OR _3_ <> USD))",
                "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1a3c1042s3dUSDo6l1l0", fourColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_OR_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8, DataType.BPCHAR);
        helper("(_1_ <> 999 AND (_2_ < 123 OR _3_ <> USD))",
                "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1a3c1042s3dUSDo6l1l0", fourColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_OR_Prune_4() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("(_0_ = bar AND _1_ <> 999)",
                "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1a3c1042s3dUSDo6l1l0", fourColumns, supportedTypes);
    }

    @Test
    public void test_AND_AND_OR_Prune_23() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.BPCHAR);
        helper("_0_ = bar",
                "a0c25s3dbaro5a1c23s3d999o6l0a2c701s3d123o1a3c1042s3dUSDo6l1l0", fourColumns, supportedTypes);
    }

    @Test
    public void test_NOT_Simple_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("NOT (_0_ = bar)", "a0c25s3dbaro5l2", oneTextColumn, supportedTypes);
    }

    @Test
    public void test_NOT_Simple_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0c25s3dbaro5l2", oneTextColumn, supportedTypes);
    }

    @Test
    public void test_AND_NOT_OR_NOT_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER, DataType.FLOAT8);
        helper("(_0_ = bar AND (NOT (_1_ <> 999) OR NOT (_2_ < 123)))",
                "a0c25s3dbaro5a1c23s3d999o6l2a2c701s3d123o1l2l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_NOT_OR_NOT_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.INTEGER, DataType.FLOAT8);
        helper("(NOT (_1_ <> 999) OR NOT (_2_ < 123))",
                "a0c25s3dbaro5a1c23s3d999o6l2a2c701s3d123o1l2l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_NOT_OR_NOT_Prune_2() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.FLOAT8);
        helper("_0_ = bar",
                "a0c25s3dbaro5a1c23s3d999o6l2a2c701s3d123o1l2l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_AND_NOT_OR_NOT_Prune_3() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT, DataType.INTEGER);
        helper("_0_ = bar",
                "a0c25s3dbaro5a1c23s3d999o6l2a2c701s3d123o1l2l1l0", threeColumns, supportedTypes);
    }

    @Test
    public void test_IS_NOT_NULL_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("_0_ IS NOT NULL", "a0o9", oneTextColumn, supportedTypes);
    }

    @Test
    public void test_IS_NOT_NULL_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("", "a0o9", oneTextColumn, supportedTypes);
    }

    @Test
    public void test_NOT_Boolean_Prune_None() throws Exception {
        supportedTypes = EnumSet.of(DataType.BOOLEAN);
        helper("NOT (_0_)", "a0c16s4dtrueo0l2", oneBooleanColumn, supportedTypes);
    }

    @Test
    public void test_NOT_Boolean_Prune_1() throws Exception {
        supportedTypes = EnumSet.of(DataType.TEXT);
        helper("", "a0c16s4dtrueo0l2", oneBooleanColumn, supportedTypes);
    }

    private void helper(String expected,
                        String filterString,
                        List<ColumnDescriptor> columns,
                        EnumSet<DataType> supportedDataTypes) throws Exception {
        Node root = new FilterParser().parse(filterString);
        SupportedDataTypePruner supportedDataTypePruner = new SupportedDataTypePruner(columns, supportedDataTypes);
        ToStringTreeVisitor toStringTreeVisitor = new ToStringTreeVisitor();
        TRAVERSER.traverse(root, supportedDataTypePruner, toStringTreeVisitor);
        assertEquals(expected, toStringTreeVisitor.toString());
    }
}