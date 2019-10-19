package me.vzhilin.dbrow.catalog;

import com.google.common.collect.Iterables;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class Catalog {
    private final Map<String, Schema> schemas = new LinkedHashMap<>();

    public Catalog() { }

    public Schema getSchema(String name) {
        return schemas.computeIfAbsent(name, Schema::new);
    }

    public boolean hasTable(String schemaName, String tableName) {
        return schemas.containsKey(schemaName) && schemas.get(schemaName).hasTable(tableName);
    }

    public Table getTable(String schemaName, String tableName) {
        return schemas.get(schemaName).getTable(tableName);
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
}
