package me.vzhilin.dbrow.adapter.mariadb;

import me.vzhilin.dbrow.adapter.ColumnType;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;
import me.vzhilin.dbrow.adapter.ColumnTypeInfo;

import java.util.*;

public class MariadbColumnTypeInfo implements ColumnTypeInfo {
    private final Map<String, ColumnTypeDescription> columnTypes = new HashMap<>();

    public MariadbColumnTypeInfo() {
        Set<String> attr = new HashSet<>();
        attr.add("SIGNED");
        attr.add("UNSIGNED");
        attr.add("ZEROFILL");

        addColumnType(new ColumnTypeDescription("enum", "enum", ColumnType.ENUM));
        addColumnType(new ColumnTypeDescription("boolean","tinyint", ColumnType.INTEGER));
        addColumnType(new ColumnTypeDescription("bit", "bit",ColumnType.INTEGER, true));
        addColumnType(new ColumnTypeDescription("binary","binary", ColumnType.BYTE_ARRAY, true));
        ColumnTypeDescription varbinary = new ColumnTypeDescription("varbinary", "varbinary", ColumnType.BYTE_ARRAY, true);
        varbinary.setMandatoryLength(true);
        addColumnType(varbinary);

        for (String t: new String[]{"smallint", "tinyint", "mediumint", "int", "bigint"}) {
            addColumnType(new ColumnTypeDescription(t, t,ColumnType.INTEGER, true, false, attr));
        }
        addColumnType(new ColumnTypeDescription("integer", "int",ColumnType.INTEGER, true, false, attr));

        for (String t: new String[]{"decimal", "dec", "numeric", "fixed"}) {
            addColumnType(new ColumnTypeDescription(t, "decimal",ColumnType.DECIMAL, true, true, attr));
        }

        for (String t: new String[]{"float", "double"}) {
            addColumnType(new ColumnTypeDescription(t,t, ColumnType.FLOAT, false, false, attr));
        }
        addColumnType(new ColumnTypeDescription("double precision", "double", ColumnType.FLOAT, false, false, attr));

        addColumnType(new ColumnTypeDescription("char","char", ColumnType.STRING, true));
        ColumnTypeDescription varcharDesc = new ColumnTypeDescription("varchar", "varchar", ColumnType.STRING, true);
        varcharDesc.setMandatoryLength(true);
        addColumnType(varcharDesc);

        for (String t: new String[]{ "tinytext", "longtext"}) {
            addColumnType(new ColumnTypeDescription(t, t, ColumnType.STRING));
        }
        addColumnType(new ColumnTypeDescription("json", "longtext", ColumnType.STRING));
        for (String t: new String[]{"date", "time", "datetime", "timestamp", "year"}) {
            addColumnType(new ColumnTypeDescription(t, t, ColumnType.DATE));
        }
    }

    private void addColumnType(ColumnTypeDescription description) {
        columnTypes.put(description.getName().toLowerCase(), description);
    }

    @Override
    public Map<String, ColumnTypeDescription> getColumnTypes() {
        return Collections.unmodifiableMap(columnTypes);
    }

    @Override
    public ColumnTypeDescription getType(String type) {
        return columnTypes.get(type.toLowerCase());
    }
}
