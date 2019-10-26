package me.vzhilin.dbrow.catalog;

public final class ForeignKeyColumn {
    private final ForeignKey fk;
    private final Column column;

    public ForeignKeyColumn(ForeignKey fk, Column column) {
        this.fk = fk;
        this.column = column;
    }

    public Column getColumn() {
        return column;
    }

    public ForeignKey getFk() {
        return fk;
    }
}
