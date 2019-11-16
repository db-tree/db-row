package me.vzhilin.dbrow.adapter.postgres;

import me.vzhilin.dbrow.adapter.BasicColumnTypeInfo;
import me.vzhilin.dbrow.adapter.ColumnType;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;
import me.vzhilin.dbrow.adapter.conv.BooleanConverter;
import me.vzhilin.dbrow.adapter.conv.UUIDConverter;

public final class PostgresColumnTypeInfo extends BasicColumnTypeInfo {
    public PostgresColumnTypeInfo() {
        addNumericTypes();
        addMonetaryTypes();
        addCharacterTypes();
        addBinaryTypes();
        addDatetimeTypes();
        addBooleanType();
        addUUIDType();
    }

    private void addUUIDType() {
        ColumnTypeDescription uuid = new ColumnTypeDescription("uuid", ColumnType.STRING);
        uuid.setConv(UUIDConverter.INSTANCE);
        addColumnType(uuid);
    }

    private void addNumericTypes() {
        addInteger("smallint").setAlias("int2");
        addInteger("integer").setAlias("int4");
        addInteger("bigint").setAlias("int8");
        addInteger("smallserial").setAlias("int2");
        addInteger("serial");
        addInteger("bigserial");

        addDecimal("decimal").setAlias("numeric");
        addFloat("real").setAlias("float4");
        addFloat("double precision").setAlias("float8");
        addDecimal("numeric").setHasLength().setHasPrecision();
    }

    private void addMonetaryTypes() {
        addColumnType(new ColumnTypeDescription("money", ColumnType.DECIMAL));
    }

    private void addCharacterTypes() {
        addString("character varying").setAlias("varchar").setMandatoryLength();
        addString("varchar").setMandatoryLength();
        addString("character").setAlias("bpchar").setMandatoryLength();
        addString("char").setAlias("bpchar").setMandatoryLength();
        addString("text");
    }

    private void addBinaryTypes() {
        addColumnType(new ColumnTypeDescription("bytea", ColumnType.BYTE_ARRAY));
    }

    private void addDatetimeTypes() {
        addDate("timestamp").setHasLength();
        addDate("timestamp with time zone").setAlias("timestamptz");
        addDate("date");
        addDate("time").setHasLength();
        addDate("time with time zone").setAlias("timetz");
        addColumnType(new ColumnTypeDescription("interval", ColumnType.INTERVAL).setHasLength());
    }

    private void addBooleanType() {
        ColumnTypeDescription bool = new ColumnTypeDescription("boolean", ColumnType.BOOLEAN);
        bool.setConv(BooleanConverter.INSTANCE);
        addColumnType(bool.setAlias("bool"));
    }
}
