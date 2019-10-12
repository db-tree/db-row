package me.vzhilin.db;

import java.util.Arrays;

public final class Key {
    private final Object[] orderedColumns;

    public Key(Object[] orderedColumns) {
        this.orderedColumns = orderedColumns;
    }

    public Object getKeyColumn(int columnIndex) {
        return orderedColumns[columnIndex];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Arrays.equals(orderedColumns, key.orderedColumns);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(orderedColumns);
    }
}
