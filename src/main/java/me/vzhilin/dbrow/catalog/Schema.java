package me.vzhilin.dbrow.catalog;

import java.util.*;
import java.util.function.Consumer;

public final class Schema {
    private final String name;
    private final Map<String, Table> tables = new LinkedHashMap<>();

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

    public void forEach(Consumer<Table> consumer) {
        tables.values().forEach(consumer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;

        if (!Objects.equals(getTableNames(), schema.getTableNames())) {
            return false;
        }

        for (String tableName: getTableNames()) {
            Table t1 = getTable(tableName);
            Table t2 = schema.getTable(tableName);

            if (!t1.equals(t2)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
