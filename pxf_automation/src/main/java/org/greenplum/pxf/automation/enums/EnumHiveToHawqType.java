package org.greenplum.pxf.automation.enums;

/**
 * Hive to Hawq types
 * Replica of PXF enum, both classes are in sync
 * @see org.greenplum.pxf.plugins.hive.utilities.EnumHiveToHawqType
 */
public enum EnumHiveToHawqType {
    StringType("string", "text"),
    TinyintType("tinyint", "int2"),
    SmallintType("smallint", "int2"),
    IntType("int", "int4"),
    BigintType("bigint", "int8"),
    BooleanType("boolean", "bool"),
    FloatType("float", "float4"),
    DoubleType("double", "float8"),
    DecimalType("decimal", "numeric"),
    BinaryType("binary", "bytea"),
    CharType("char", "bpchar"),
    ArrayType("array", "text", "[<,>]"),
    MapType("map", "text", "[<,>]"),
    StructType("struct", "text", "[<,>]"),
    UnionType("uniontype", "text", "[<,>]");

    private String hiveType;
    private String hawqTypeName;
    private String splitExpression;

    EnumHiveToHawqType(String hiveType, String hawqTypeName) {
        this.hiveType = hiveType;
        this.hawqTypeName = hawqTypeName;
    }

    EnumHiveToHawqType(String hiveType, String hawqTypeName, String splitExpression) {
        this(hiveType, hawqTypeName);
        this.splitExpression = splitExpression;
    }

    public String getHiveType() {
        return this.hiveType;
    }

    public String getHawqType() {
        return this.hawqTypeName;
    }

    public String getSplitExpression() {
        return this.splitExpression;
    }

    /**
     * Converts Hive to Hawq type
     *
     * @param hiveType Hive type
     * @return corresponding Hawq type
     * @throws Exception
     */
    public static String getHawqType(String hiveType) {
        if (hiveType == null || hiveType.length() == 0)
            throw new RuntimeException("Unable to map Hive's type, empty type was passed.");
        for (EnumHiveToHawqType t : values()) {
            String hiveTypeName = hiveType;
            if (t.getSplitExpression() != null) {
                String[] tokens = hiveType.split(t.getSplitExpression());
                hiveTypeName = tokens[0];
            }
            if (t.getHiveType().toLowerCase().equals(hiveTypeName.toLowerCase())) {
                return t.getHawqType();
            }
        }
        throw new RuntimeException("Unable to map Hive's type: " + hiveType
                + " to GPDB's type");
    }

}