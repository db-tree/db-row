package me.vzhilin.catalog;

import java.util.*;
import java.util.function.Consumer;

public final class Schema {
    private final String name;
    private final Map<String, Table> tables = new HashMap<>();

    public Schema(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Table addTable(String tableName) {
        Table newTable = new Table(this, tableName);
        tables.put(tableName, newTable);
        return newTable;
    }

    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    public Set<String> getTableNames() {
        return Collections.unmodifiableSet(tables.keySet());
    }

    public boolean hasTable(String tableName) {
        return tables.containsKey(tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return name.equals(schema.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public void forEach(Consumer<Table> consumer) {
        tables.values().forEach(consumer);
    }
}
