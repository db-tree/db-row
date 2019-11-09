package me.vzhilin.dbrow.catalog;

import com.google.common.collect.Iterables;

import java.util.*;
import java.util.function.Consumer;

public final class Catalog {
    private final Map<String, Schema> schemas = new LinkedHashMap<>();

    public Catalog() { }

    public Schema getSchema(String name) {
        return schemas.computeIfAbsent(name, Schema::new);
    }

    public Set<String> getSchemaNames() {
        return Collections.unmodifiableSet(schemas.keySet());
    }

    public boolean hasTable(String schemaName, String tableName) {
        return schemas.containsKey(schemaName) && schemas.get(schemaName).hasTable(tableName);
    }

    public Table getTable(String schemaName, String tableName) {
        return schemas.computeIfAbsent(schemaName, Schema::new).getTable(tableName);
    }

    public void forEachTable(Consumer<Table> consumer) {
        schemas.forEach((name, schema) -> schema.forEach(consumer));
    }

    public void forEachSchema(Consumer<Schema> schemaConsumer) {
        schemas.values().forEach(schemaConsumer);
    }

    public Schema getOnlySchema() {
        return Iterables.getOnlyElement(schemas.values());
    }

    public Schema addSchema(String schemaName) {
        Schema schema = new Schema(schemaName);
        schemas.put(schemaName, schema);
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Catalog catalog = (Catalog) o;

        if (!Objects.equals(getSchemaNames(), catalog.getSchemaNames())) {
            return false;
        }

        for (String schemaName: getSchemaNames()) {
            if (!getSchema(schemaName).equals(catalog.getSchema(schemaName))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemas);
    }
}
