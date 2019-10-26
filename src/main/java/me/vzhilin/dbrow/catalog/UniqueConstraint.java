package me.vzhilin.dbrow.catalog;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public final class UniqueConstraint {
    private final Table table;
    private final String constraintName;
    private final Set<ForeignKey> foreignKeys = new HashSet<>();
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
        return columns.stream().filter(ucc -> name.equals(ucc.getName())).findFirst().get();
    }

    public Set<UniqueConstraintColumn> getColumns() {
        return Collections.unmodifiableSet(columns);
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
}
