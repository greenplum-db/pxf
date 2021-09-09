package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Schema;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

public enum EnumAvroTypeConverter {
    BOOLEAN {
        @Override
        public DataType getDataType() {
            return DataType.BOOLEAN;
        }
    },
    BYTES {
        @Override
        public DataType getDataType() {
            return DataType.BYTEA;
        }
    },
    DOUBLE {
        @Override
        public DataType getDataType() {
            return DataType.FLOAT8;
        }
    },
    FLOAT {
        @Override
        public DataType getDataType() {
            return DataType.REAL;
        }
    },
    INT {
        @Override
        public DataType getDataType() {
            return DataType.INTEGER;
        }
    },
    LONG {
        @Override
        public DataType getDataType() {
            return DataType.BIGINT;
        }
    },
    STRING {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }
    },
    ARRAY {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }
    },
    MAP {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }
    },
    ENUM {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }
    },
    FIXED {
        @Override
        public DataType getDataType() {
            return DataType.BYTEA;
        }
    },
    RECORD {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }
    },
    UNION {
        @Override
        public DataType getDataType() {
            // this should not be called if using AvroTypeConverter#from()
            return null;
        }
    };

    public static EnumAvroTypeConverter from(Schema schema) {
        Schema.Type type = schema.getType();

        if (type == Schema.Type.UNION) {
            Schema.Type nestedType = schema.getTypes().get(0).getType();
            // make sure to get the non-null type
            type = (nestedType == Schema.Type.NULL) ?
                    schema.getTypes().get(1).getType() :
                    nestedType;
        }

        return valueOf(type.name());
    }

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType();

    public static List<ColumnDescriptor> getColumnDescriptorsFromSchema(Schema schema) {
        return schema
                .getFields()
                .stream()
                .map(f -> new ColumnDescriptor(
                        f.name(),
                        from(f.schema()).getDataType().getOID(),
                        1,
                        "",
                        new Integer[]{}
                )).collect(Collectors.toList());
    }
}
