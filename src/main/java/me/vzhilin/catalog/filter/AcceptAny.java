package me.vzhilin.catalog.filter;


import me.vzhilin.catalog.CatalogFilter;

public final class AcceptAny implements CatalogFilter {
    @Override
    public boolean acceptSchema(String schemaName) {
        return true;
    }

    @Override
    public boolean acceptTable(String schemaName, String tableName) {
        return true;
    }
}
