package me.vzhilin.catalog;

import java.sql.JDBCType;
import java.util.Objects;

public final class Column {
    private final String name;
    private final String dataType;
    private final Table table;
    private final int index;
    private final JDBCType jdbcType;

    public Column(Table table, String name, String dataType, int index, JDBCType jdbcType) {
        this.table = table;
        this.name = name;
        this.dataType = dataType;
        this.index = index;
        this.jdbcType = jdbcType;
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return name.equals(column.name) &&
                dataType.equals(column.dataType) &&
                table.equals(column.table);
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, table);
    }

    @Override
    public String toString() {
        return table.getSchemaName() + "." + table.getName() + "." + name;
    }
}
