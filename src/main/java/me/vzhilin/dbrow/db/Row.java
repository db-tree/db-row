package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.catalog.*;

import java.util.*;

public final class Row {
    private final ObjectKey key;
    private final RowContext ctx;
    private final Map<Column, Object> values = new LinkedHashMap<>();
    private final Map<ForeignKey, Row> forwardReferences = new LinkedHashMap<>();
    private final Map<ForeignKey, Number> backwardReferencesCount = new LinkedHashMap<>();
    private boolean loaded;
    private boolean backwardReferencesLoaded;

    public Row(RowContext ctx, ObjectKey key) {
        this.ctx = ctx;
        this.key = key;
    }

    public RowContext getContext() {
        return ctx;
    }

    public Object get(Column column) {
        ensureLoaded();

        return values.get(column);
    }

    public Object get(String column) {
        return get(key.getTable().getColumn(column));
    }

    public Table getTable() {
        return key.getTable();
    }

    public Map<Column, Object> getValues() {
        ensureLoaded();
        return Collections.unmodifiableMap(values);
    }

    public Map<PrimaryKeyColumn, Object> getKeyValues() {
        HashMap<PrimaryKeyColumn, Object> rs = new LinkedHashMap<>();
        PrimaryKey pk = key.getTable().getPrimaryKey().get();
        pk.getColumns().forEach(pkc -> rs.put(pkc, values.get(pkc.getColumn())));
        return rs;
    }

    // TODO cache references
    public Map<ForeignKey, Row> forwardReferences() {
        ensureLoaded();
        return Collections.unmodifiableMap(forwardReferences);
    }

    public Map<ForeignKey, Number> backwardReferencesCount() {
        if (!backwardReferencesLoaded) {
            Set<ForeignKey> foreignKeys = key.getTable().getPrimaryKey().get().getForeignKeys();
            foreignKeys.forEach(fk -> backwardReferencesCount.put(fk, ctx.backReferencesCount(this, fk)));
            backwardReferencesLoaded = true;
        }

        return Collections.unmodifiableMap(backwardReferencesCount);
    }

    public Iterable<Row> backwardReference(ForeignKey fk) {
        return ctx.backReferences(this, fk);
    }

    private void ensureLoaded() {
        if (!loaded) {
            loaded = true;
            values.putAll(ctx.fetchValues(key));

            for (ForeignKey fk: key.getTable().getForeignKeys().values()) {
                Row reference = loadReference(fk);
                if (reference == null) {
                    continue;
                }
                forwardReferences.put(fk, reference);
            }
        }
    }

    public Row forwardReference(ForeignKey fk) {
        ensureLoaded();
        return forwardReferences.get(fk);
    }

    public Row loadReference(ForeignKey fk) {
        Object[] keyColumns = new Object[fk.getPkTable().getPrimaryKey().get().getColumnCount()];

        final boolean[] hasNull = {false};
        fk.getColumnMapping().forEach((pkColumn, fkColumn) -> {
            Object value = get(fkColumn.getColumn());
            if (!hasNull[0] && value != null) {
                keyColumns[pkColumn.getPrimaryKeyIndex()] = value;
            } else {
                hasNull[0] = true;
            }
        });
        if (hasNull[0]) {
            return null;
        }
        ObjectKey key = new ObjectKey(fk.getPkTable(), new Key(keyColumns));
        if (ctx.exists(key)) {
            return new Row(ctx, key);
        } else {
            return null;
        }
    }

    public ObjectKey getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return key.equals(row.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "Row{" +
                "key=" + key +
                '}';
    }
}
