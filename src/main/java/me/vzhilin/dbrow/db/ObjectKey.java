package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.catalog.UniqueConstraintColumn;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

public final class ObjectKey {
    private final Map<UniqueConstraintColumn, Object> key;
    private final UniqueConstraint cons;

    public ObjectKey( UniqueConstraint cons, Map<UniqueConstraintColumn, Object> key) {
        this.cons = cons;
        this.key = key;
    }

    public UniqueConstraint getCons() {
        return cons;
    }

    public Table getTable() {
        return cons.getTable();
    }

    public Map<UniqueConstraintColumn, Object> getKey() {
        return Collections.unmodifiableMap(key);
    }

    public void forEach(BiConsumer<UniqueConstraintColumn, Object> action) {
        key.forEach(action);
    }
}
