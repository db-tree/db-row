package me.vzhilin.dbrow.catalog;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForeignKeyColumn that = (ForeignKeyColumn) o;
        return  fk.getTable().getSchemaName().equals(that.fk.getTable().getSchemaName()) &&
                fk.getTable().getName().equals(that.fk.getTable().getName()) &&
                Objects.equals(fk.getFkName(), that.fk.getFkName()) &&
                column.equals(that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fk.getTable().getSchemaName(), fk.getTable().getName(), fk.getFkName(), column);
    }
}
