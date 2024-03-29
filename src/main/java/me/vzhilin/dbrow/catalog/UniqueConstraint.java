package me.vzhilin.dbrow.catalog;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class UniqueConstraint {
    private final Table table;
    private final String constraintName;
    private final Set<ForeignKey> foreignKeys = new LinkedHashSet<>();
    private final Set<UniqueConstraintColumn> columns;

    public UniqueConstraint(Table table, Set<UniqueConstraintColumn> columns) {
        this(table, columns, "");
    }

    public UniqueConstraint(Table table, Set<UniqueConstraintColumn> columns, String constraintName) {
        this.table = table;
        this.columns = columns;
        this.constraintName = constraintName;
    }

    public UniqueConstraintColumn getColumn(String name) {
        // TODO NPE
        return columns.stream().filter(ucc -> name.equals(ucc.getName())).findFirst().get();
    }

    public Set<UniqueConstraintColumn> getColumns() {
        return Collections.unmodifiableSet(columns);
    }

    public void addForeignKey(ForeignKey fk) {
        foreignKeys.add(fk);
    }

    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(foreignKeys);
    }

    public int getColumnCount() {
        return columns.size();
    }

    public Table getTable() {
        return table;
    }

    public Set<String> getColumnNames() {
        return columns.stream().map(UniqueConstraintColumn::getName).collect(Collectors.toSet());
    }

    public String getName() {
        return constraintName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniqueConstraint that = (UniqueConstraint) o;
        return
                table.getSchemaName().equals(that.table.getSchemaName()) &&
                table.getName().equals(that.table.getName()) &&
                Objects.equals(constraintName, that.constraintName) &&
                columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table.getSchemaName(), table.getName(), constraintName, columns);
    }

    @Override
    public String toString() {
        return "UniqueConstraint{" +
                "table=" + table +
                ", constraintName='" + constraintName + '\'' +
                '}';
    }
}
