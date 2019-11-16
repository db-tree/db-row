package me.vzhilin.dbrow.adapter.oracle;

import me.vzhilin.dbrow.adapter.ColumnType;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;
import me.vzhilin.dbrow.adapter.ColumnTypeInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OracleColumnTypeInfo implements ColumnTypeInfo {
    private final Map<String, ColumnTypeDescription> columnTypes = new HashMap<>();

    public OracleColumnTypeInfo() {
        addColumnType(new ColumnTypeDescription("VARCHAR2", ColumnType.STRING).setMandatoryLength());
        addColumnType(new ColumnTypeDescription("NVARCHAR2", ColumnType.STRING).setMandatoryLength());

        addColumnType(new ColumnTypeDescription("CHARACTER VARYING", ColumnType.STRING).setAlias("VARCHAR2").setMandatoryLength());
        addColumnType(new ColumnTypeDescription("CHAR VARYING", ColumnType.STRING).setAlias("VARCHAR2").setMandatoryLength());

        ColumnTypeDescription number = new ColumnTypeDescription("NUMBER", ColumnType.DECIMAL);
        number.setHasLength();
        number.setHasPrecision();
        addColumnType(number);

        addColumnType(new ColumnTypeDescription("INTEGER", ColumnType.INTEGER).setAlias("NUMBER"));
        addColumnType(new ColumnTypeDescription("INT", ColumnType.INTEGER).setAlias("NUMBER"));
        addColumnType(new ColumnTypeDescription("SMALLINT", ColumnType.INTEGER).setAlias("NUMBER"));
        addColumnType(new ColumnTypeDescription("FLOAT", ColumnType.FLOAT).setHasLength());
        addColumnType(new ColumnTypeDescription("REAL", ColumnType.FLOAT).setAlias("FLOAT"));
        addColumnType(new ColumnTypeDescription("DOUBLE PRECISION", ColumnType.FLOAT).setAlias("FLOAT"));

        addColumnType(new ColumnTypeDescription("BINARY_FLOAT", ColumnType.FLOAT));
        addColumnType(new ColumnTypeDescription("BINARY_DOUBLE",ColumnType.FLOAT));
        addColumnType(new ColumnTypeDescription("DATE", ColumnType.DATE));
        addColumnType(new ColumnTypeDescription("TIMESTAMP", ColumnType.DATE).setHasLength());
        addColumnType(new ColumnTypeDescription("RAW", ColumnType.BYTE_ARRAY).setMandatoryLength());
        addColumnType(new ColumnTypeDescription("LONG RAW", ColumnType.BYTE_ARRAY));
        addColumnType(new ColumnTypeDescription("CHAR",  ColumnType.STRING).setHasLength());
        addColumnType(new ColumnTypeDescription("NCHAR", ColumnType.STRING).setHasLength());
        addColumnType(new ColumnTypeDescription("CLOB", ColumnType.STRING));
        addColumnType(new ColumnTypeDescription("NCLOB", ColumnType.STRING));
        addColumnType(new ColumnTypeDescription("BLOB", ColumnType.BYTE_ARRAY));
        addColumnType(new ColumnTypeDescription("BFILE",ColumnType.BYTE_ARRAY));

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
