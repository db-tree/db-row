package me.vzhilin.dbrow.adapter.postgres;

import me.vzhilin.dbrow.adapter.ColumnType;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;
import me.vzhilin.dbrow.adapter.ColumnTypeInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PostgresColumnTypeInfo implements ColumnTypeInfo {
    private final Map<String, ColumnTypeDescription> columnTypes = new HashMap<>();

    public PostgresColumnTypeInfo() {
        addNumericTypes();
        addMonetaryTypes();
        addCharacterTypes();
        addBinaryTypes();
        addDatetimeTypes();
        addBooleanType();
    }

    private void addNumericTypes() {
        addColumnType(new ColumnTypeDescription("smallint", ColumnType.INTEGER).setAlias("int2"));
        addColumnType(new ColumnTypeDescription("integer", ColumnType.INTEGER).setAlias("int4"));
        addColumnType(new ColumnTypeDescription("bigint", ColumnType.INTEGER).setAlias("int8"));

        addColumnType(new ColumnTypeDescription("decimal", ColumnType.DECIMAL).setAlias("numeric"));

        addColumnType(new ColumnTypeDescription("real", ColumnType.FLOAT).setAlias("float4"));
        addColumnType(new ColumnTypeDescription("double precision", ColumnType.FLOAT).setAlias("float8"));

        addColumnType(new ColumnTypeDescription("smallserial", ColumnType.INTEGER).setAlias("int2"));
        addColumnType(new ColumnTypeDescription("serial", ColumnType.INTEGER));
        addColumnType(new ColumnTypeDescription("bigserial", ColumnType.INTEGER));

        addColumnType(new ColumnTypeDescription("numeric", ColumnType.DECIMAL).setHasLength().setHasPrecision());
    }

    private void addMonetaryTypes() {
        addColumnType(new ColumnTypeDescription("money", ColumnType.DECIMAL));
    }

    private void addCharacterTypes() {
        addColumnType(new ColumnTypeDescription("character varying", ColumnType.STRING).setAlias("varchar").setMandatoryLength());
        addColumnType(new ColumnTypeDescription("varchar", ColumnType.STRING).setMandatoryLength());
        addColumnType(new ColumnTypeDescription("character", ColumnType.STRING).setAlias("bpchar").setMandatoryLength());
        addColumnType(new ColumnTypeDescription("char", ColumnType.STRING).setAlias("bpchar").setMandatoryLength());
        addColumnType(new ColumnTypeDescription("text", ColumnType.STRING));
    }

    private void addBinaryTypes() {
        addColumnType(new ColumnTypeDescription("bytea", ColumnType.BYTE_ARRAY));
    }

    private void addDatetimeTypes() {
        addColumnType(new ColumnTypeDescription("timestamp", ColumnType.DATE).setHasLength());
        addColumnType(new ColumnTypeDescription("timestamp with time zone", ColumnType.DATE).setAlias("timestamptz"));
        addColumnType(new ColumnTypeDescription("date", ColumnType.DATE));
        addColumnType(new ColumnTypeDescription("time", ColumnType.DATE).setHasLength());
        addColumnType(new ColumnTypeDescription("time with time zone", ColumnType.DATE).setAlias("timetz"));
        addColumnType(new ColumnTypeDescription("interval", ColumnType.INTERVAL).setHasLength());
    }

    private void addBooleanType() {
        addColumnType(new ColumnTypeDescription("boolean", ColumnType.BOOLEAN).setAlias("bool"));
    }

    private void addColumnType(ColumnTypeDescription description) {
        columnTypes.put(description.getName().toLowerCase(), description);
    }

    @Override
    public ColumnTypeDescription getType(String type) {
        return columnTypes.get(type);
    }

    @Override
    public Map<String, ColumnTypeDescription> getColumnTypes() {
        return Collections.unmodifiableMap(columnTypes);
    }
}
