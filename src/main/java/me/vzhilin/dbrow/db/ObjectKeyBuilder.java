package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.catalog.UniqueConstraintColumn;

import java.util.HashMap;
import java.util.Map;

public final class ObjectKeyBuilder {
    private final Map<UniqueConstraintColumn, Object> keyColumns;
    private final UniqueConstraint uniq;

    public ObjectKeyBuilder(UniqueConstraint uniq) {
        this.uniq = uniq;
        this.keyColumns = new HashMap<>();
    }

    public ObjectKeyBuilder set(String columnName, Object value) {
        keyColumns.put(uniq.getColumn(columnName), value);
        return this;
    }

    public ObjectKey build() {
        // TODO check if all values are not null
        return new ObjectKey(uniq, keyColumns);
    }
}
