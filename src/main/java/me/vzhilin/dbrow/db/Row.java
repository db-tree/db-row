package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.adapter.RowValue;
import me.vzhilin.dbrow.catalog.*;

import java.util.*;

public final class Row {
    private final ObjectKey key;
    private final RowContext ctx;
    private final Map<Column, RowValue> values = new LinkedHashMap<>();
    private final Map<ForeignKey, Row> forwardReferences = new LinkedHashMap<>();
    private final Map<UniqueConstraint, Map<ForeignKey, Number>> backwardReferencesCount = new LinkedHashMap<>();
    private boolean loaded;
    private boolean backwardReferencesLoaded;

    public Row(RowContext ctx, ObjectKey key) {
        this.ctx = ctx;
        this.key = key;
    }

    public RowContext getContext() {
        return ctx;
    }

    public RowValue get(Column column) {
        ensureLoaded();

        return values.get(column);
    }

    public RowValue get(String column) {
        return get(key.getTable().getColumn(column));
    }

    public Table getTable() {
        return key.getTable();
    }

    public Map<Column, RowValue> getValues() {
        ensureLoaded();
        return Collections.unmodifiableMap(values);
    }

    public Map<UniqueConstraintColumn, RowValue> getKeyValues() {
        ensureLoaded();
        HashMap<UniqueConstraintColumn, RowValue> rs = new LinkedHashMap<>();
        UniqueConstraint pk = key.getCons();
        pk.getColumns().forEach(pkc -> rs.put(pkc, values.get(pkc.getColumn())));
        return rs;
    }

    // TODO cache references
    public Map<ForeignKey, Row> forwardReferences() {
        ensureLoaded();
        return Collections.unmodifiableMap(forwardReferences);
    }


    public Map<UniqueConstraint, Map<ForeignKey, Number>> backwardReferencesCount() {
        if (!backwardReferencesLoaded) {
            for (UniqueConstraint ucc: getTable().getUniqueConstraints()) {
                Map<ForeignKey, Number> count = new LinkedHashMap<>();
                for (ForeignKey fk: ucc.getForeignKeys()) {
                    count.put(fk, ctx.backReferencesCount(this, fk));
                }
                backwardReferencesCount.put(ucc, count);
            }

            backwardReferencesLoaded = true;
        }

        return Collections.unmodifiableMap(backwardReferencesCount);
    }

    private Set<ForeignKey> getForeignKeys() {
        Set<ForeignKey> foreignKeys = new HashSet<>();
        for (UniqueConstraint uniq: key.getTable().getUniqueConstraints()) {
            foreignKeys.addAll(uniq.getForeignKeys());
        }
        return foreignKeys;
    }

    public Iterable<Row> backwardReference(ForeignKey fk) {
        return ctx.backReferences(this, fk);
    }

    private void ensureLoaded() {
        if (!loaded) {
            loaded = true;
            values.putAll(ctx.fetchValues(key));

            for (ForeignKey fk: key.getTable().getForeignKeys()) {
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
        UniqueConstraint uniq = fk.getUniqueConstraint();
        Map<UniqueConstraintColumn, Object> keyColumns = new HashMap<>();

        final boolean[] hasNull = {false};
        fk.getColumnMapping().forEach((pkColumn, fkColumn) -> {
            RowValue value = get(fkColumn.getColumn());
            if (!hasNull[0] && value != null) {
                keyColumns.put(pkColumn, value.get());
            } else {
                hasNull[0] = true;
            }
        });
        if (hasNull[0]) {
            return null;
        }
        ObjectKey key = new ObjectKey(uniq, keyColumns);
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
