package me.vzhilin.dbrow.catalog;

public final class PrimaryKeyColumn {
    private final Column column;
    private final int primaryKeyIndex;
    private final PrimaryKey pk;

    public PrimaryKeyColumn(PrimaryKey pk, Column column, int primaryKeyIndex) {
        this.pk = pk;
        this.column = column;
        this.primaryKeyIndex = primaryKeyIndex;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }

    public Column getColumn() {
        return column;
    }

    public String getName() {
        return column.getName();
    }
}
