package me.vzhilin.dbrow.catalog;

public class UniqueConstraintColumn {
    private final Column column;

    public UniqueConstraintColumn(Column column) {
        this.column = column;
    }

    public String getName() {
        return column.getName();
    }

    public Column getColumn() {
        return column;
    }
}
