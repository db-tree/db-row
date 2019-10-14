package me.vzhilin.catalog;

public interface CatalogFilter {
    boolean acceptSchema(String schemaName);
    boolean acceptTable(String schemaName, String tableName);
}
