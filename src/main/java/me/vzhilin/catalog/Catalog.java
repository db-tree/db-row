package me.vzhilin.catalog;

import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class Catalog {
    private final Map<String, Schema> schemas = new HashMap<>();

    public Catalog() { }

    public Schema getSchema(String name) {
        return schemas.computeIfAbsent(name, Schema::new);
    }

    public void forEachTable(Consumer<Table> consumer) {
        schemas.forEach((name, schema) -> schema.forEach(consumer));
    }

    public Schema getOnlySchema() {
        return Iterables.getOnlyElement(schemas.values());
    }
}
