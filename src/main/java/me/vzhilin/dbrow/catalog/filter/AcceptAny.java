package me.vzhilin.dbrow.catalog.filter;
import me.vzhilin.dbrow.catalog.CatalogFilter;

public final class AcceptAny implements CatalogFilter {
    @Override
    public boolean acceptSchema(String schemaName) {
        return true;
    }

    @Override
    public boolean acceptTable(String schemaName, String tableName) {
        return true;
    }

    @Override
    public boolean acceptColumn(String schema, String table, String column) {
        return true;
    }
}
