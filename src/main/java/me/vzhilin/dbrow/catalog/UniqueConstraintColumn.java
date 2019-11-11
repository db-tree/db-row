package me.vzhilin.dbrow.catalog;

import com.google.common.base.Preconditions;

import java.util.Objects;

public final class UniqueConstraintColumn {
    private final Column column;
    private final int position;

    public UniqueConstraintColumn(Column column, int position) {
        Preconditions.checkNotNull(column);
        this.column = column;
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    public String getName() {
        return column.getName();
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniqueConstraintColumn that = (UniqueConstraintColumn) o;
        return position == that.position &&
                column.equals(that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, position);
    }
}
