package me.vzhilin.dbrow.catalog;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.util.BiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

public final class ForeignKey {
    private final String fkName;
    private final Table table;
    private final UniqueConstraint uniqueConstraint;
    private BiMap<UniqueConstraintColumn, ForeignKeyColumn> fkMapping;

    public ForeignKey(String fkName, Table table, UniqueConstraint unique) {
        this.fkName = fkName;
        this.table = table;
        this.uniqueConstraint = unique;
    }

    public String getFkName() {
        return fkName;
    }

    public Table getTable() {
        return table;
    }

    public UniqueConstraint getUniqueConstraint() {
        return uniqueConstraint;
    }

    public BiMap<UniqueConstraintColumn, ForeignKeyColumn> getFkMapping() {
        return fkMapping; // TODO unmodifiable
    }

    public String getFkAsString() {
        List<String> columns = new ArrayList<>();
        fkMapping.forEach(new BiConsumer<UniqueConstraintColumn, ForeignKeyColumn>() {
            @Override
            public void accept(UniqueConstraintColumn primaryKeyColumn, ForeignKeyColumn foreignKeyColumn) {
                columns.add(foreignKeyColumn.getColumn().getName());
            }
        });
        return Joiner.on(',').join(columns);
    }

    /**
     * @return pkColumn -&gt; fkColumn
     */
    public BiMap<UniqueConstraintColumn, ForeignKeyColumn> getColumnMapping() {
        return fkMapping;
    }

    public int size() {
        return fkMapping.size();
    }

    public void setMapping(BiMap<UniqueConstraintColumn, ForeignKeyColumn> fkMapping) {
        this.fkMapping = fkMapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForeignKey that = (ForeignKey) o;
        return Objects.equals(fkName, that.fkName) &&
                table.getSchemaName().equals(that.table.getSchemaName()) &&
                table.getName().equals(that.table.getName()) &&
                uniqueConstraint.equals(that.uniqueConstraint) &&
                fkMapping.equals(that.fkMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fkName, table.getSchemaName(), table.getName(), uniqueConstraint, fkMapping);
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
                "fkName='" + fkName + '\'' +
                ", table=" + table +
                '}';
    }
}
