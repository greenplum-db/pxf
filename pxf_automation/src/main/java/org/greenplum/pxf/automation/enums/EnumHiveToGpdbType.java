package org.greenplum.pxf.automation.enums;

/**
 * Hive to Gpdb types
 * Replica of PXF enum, both classes are in sync
 * @see org.greenplum.pxf.plugins.hive.utilities.EnumHiveToGpdbType
 */
public enum EnumHiveToGpdbType {
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
    private String gpdbTypeName;
    private String splitExpression;

    EnumHiveToGpdbType(String hiveType, String gpdbTypeName) {
        this.hiveType = hiveType;
        this.gpdbTypeName = gpdbTypeName;
    }

    EnumHiveToGpdbType(String hiveType, String gpdbTypeName, String splitExpression) {
        this(hiveType, gpdbTypeName);
        this.splitExpression = splitExpression;
    }

    public String getHiveType() {
        return this.hiveType;
    }

    public String getGpdbType() {
        return this.gpdbTypeName;
    }

    public String getSplitExpression() {
        return this.splitExpression;
    }

    /**
     * Converts Hive to Gpdb type
     *
     * @param hiveType Hive type
     * @return corresponding Gpdb type
     * @throws Exception
     */
    public static String getGpdbType(String hiveType) {
        if (hiveType == null || hiveType.length() == 0)
            throw new RuntimeException("Unable to map Hive's type, empty type was passed.");
        for (EnumHiveToGpdbType t : values()) {
            String hiveTypeName = hiveType;
            if (t.getSplitExpression() != null) {
                String[] tokens = hiveType.split(t.getSplitExpression());
                hiveTypeName = tokens[0];
            }
            if (t.getHiveType().toLowerCase().equals(hiveTypeName.toLowerCase())) {
                return t.getGpdbType();
            }
        }
        throw new RuntimeException("Unable to map Hive's type: " + hiveType
                + " to GPDB's type");
    }

}