package me.vzhilin.dbrow.catalog;

public interface CatalogFilter {
    boolean acceptSchema(String schema);
    boolean acceptTable(String schema, String table);
    boolean acceptColumn(String schema, String table, String column);
}
