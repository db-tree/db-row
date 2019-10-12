package me.vzhilin.db;

import me.vzhilin.catalog.PrimaryKeyColumn;
import me.vzhilin.catalog.Table;

import java.util.function.BiConsumer;

public final class ObjectKey {
    private final Table table;
    private final Key key;

    public ObjectKey(Table table, Key key) {
        this.table = table;
        this.key = key;
    }

    public Table getTable() {
        return table;
    }

    public Key getKey() {
        return key;
    }

    public void forEach(BiConsumer<PrimaryKeyColumn, Object> action) {
        for (PrimaryKeyColumn pkc: table.getPrimaryKey().get().getColumns()) {
            int columnIndex = pkc.getPrimaryKeyIndex();
            action.accept(pkc, key.getKeyColumn(columnIndex));
        }
    }
}
