package me.vzhilin.db;

import me.vzhilin.catalog.PrimaryKey;
import me.vzhilin.catalog.Table;

public final class ObjectKeyBuilder {
    private final Object[] keyColumns;
    private final Table table;
    private final PrimaryKey primaryKey;

    public ObjectKeyBuilder(Table table) {
        this.table = table;
        this.primaryKey = table.getPrimaryKey().get();
        this.keyColumns = new Object[table.getPrimaryKey().get().getColumnCount()];
    }

    public ObjectKeyBuilder set(String columnName, Object value) {
        keyColumns[primaryKey.getColumn(columnName).getPrimaryKeyIndex()] = value;
        return this;
    }

    public ObjectKey build() {
        // TODO check if all columns not null
        return new ObjectKey(table, new Key(keyColumns));
    }
}
