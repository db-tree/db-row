package me.vzhilin.dbrow.catalog;

public final class UniqueConstraintColumn {
    private final Column column;
    private final int position;

    public UniqueConstraintColumn(Column column, int position) {
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
}
