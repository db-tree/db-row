package me.vzhilin.dbrow.catalog.filter;


import me.vzhilin.dbrow.catalog.CatalogFilter;

import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

public final class AcceptSchema implements CatalogFilter {
    private final Set<String> schemaNames;

    public AcceptSchema(Set<String> schemaNames) {
        this.schemaNames = schemaNames;
    }

    public AcceptSchema(String currentSchema) {
        this(newHashSet(currentSchema));
    }

    @Override
    public boolean acceptSchema(String schemaName) {
        return schemaNames.contains(schemaName);
    }

    @Override
    public boolean acceptTable(String schemaName, String tableName) {
        return acceptSchema(schemaName);
    }

    @Override
    public boolean acceptColumn(String schema, String table, String column) {
        return acceptTable(schema, table);
    }
}
