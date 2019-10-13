package me.vzhilin.catalog;

import java.util.*;

import static java.util.stream.Collectors.toSet;

public final class PrimaryKey {
    private final Optional<String> name;
    private final Table table;
    private final Map<String, PrimaryKeyColumn> columns = new LinkedHashMap<>();
    private final Set<ForeignKey> foreignKeys = new HashSet<>();

    public PrimaryKey(Optional<String> name, Table table) {
        this.name = name;
        this.table = table;
    }

    public PrimaryKeyColumn getColumn(String name) {
        return columns.get(name); // TODO notNullCheck
    }

    public boolean hasColumn(String name) {
        return columns.containsKey(name);
    }

    public void addColumn(Column column, int keySeq) {
        columns.put(column.getName(), new PrimaryKeyColumn(this, column, keySeq));
    }

    public void addForeignKey(ForeignKey foreignKey) {
        foreignKeys.add(foreignKey);
    }

    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(foreignKeys);
    }

    public Collection<PrimaryKeyColumn> getColumns() {
        return Collections.unmodifiableCollection(columns.values());
    }

    public Set<String> getColumnNames() {
        return getColumns().stream().map(pkc -> pkc.getColumn().getName()).collect(toSet());
    }

    public int getColumnCount() {
        return columns.size();
    }
}
