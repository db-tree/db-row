package me.vzhilin.catalog;

public final class ForeignKeyColumn {
    private final ForeignKey fk;
    private final Column column;
    private final int foreignKeyIndex;

    public ForeignKeyColumn(ForeignKey fk, Column column, int foreignKeyIndex) {
        this.fk = fk;
        this.column = column;
        this.foreignKeyIndex = foreignKeyIndex;
    }

    public Column getColumn() {
        return column;
    }

    public ForeignKey getFk() {
        return fk;
    }

    public int getForeignKeyIndex() {
        return foreignKeyIndex;
    }
}
