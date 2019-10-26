package me.vzhilin.dbrow.catalog;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class Column {
    private final String name;
    private final String dataType;
    private final Table table;
    private final int index;
    private final Set<ForeignKey> foreignKeys = new HashSet<>();

    public Column(Table table, String name, String dataType, int index) {
        this.table = table;
        this.name = name;
        this.dataType = dataType;
        this.index = index;
    }

    public String getSchema() {
        return table.getSchemaName();
    }

    public String getTableName() {
        return table.getName();
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public void addForeignKey(ForeignKey fk) {
        foreignKeys.add(fk);
    }

    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(foreignKeys);
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
