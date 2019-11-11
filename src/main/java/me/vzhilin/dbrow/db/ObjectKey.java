package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.catalog.UniqueConstraintColumn;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

public final class ObjectKey {
    private final Map<UniqueConstraintColumn, Object> key;
    private final UniqueConstraint cons;

    public ObjectKey(UniqueConstraint cons, Map<UniqueConstraintColumn, Object> key) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectKey objectKey = (ObjectKey) o;
        return key.equals(objectKey.key) &&
                cons.equals(objectKey.cons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, cons);
    }
}
