package me.vzhilin.dbrow.catalog.loader;

import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.CatalogFilter;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.filter.AcceptAny;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;

public abstract class MetadataCatalogLoader implements CatalogLoader {
    @Override
    public Catalog load(DataSource ds) throws SQLException {
        return load(ds, new AcceptAny());
    }

    @Override
    public Catalog load(DataSource ds, CatalogFilter filter) throws SQLException {
        Catalog catalog = new Catalog();
        try (Connection conn = ds.getConnection()) {
            QueryRunner runner = new QueryRunner();
            DatabaseMetaData metadata = conn.getMetaData();
            loadCatalog(catalog, conn, runner, filter, metadata);
        }

        return catalog;
    }

    protected void loadCatalog(Catalog catalog, Connection conn, QueryRunner runner, CatalogFilter filter, DatabaseMetaData metadata) throws SQLException {
        loadSchemas(catalog, filter, metadata);
        loadTables(catalog, filter, metadata);
        loadColumns(catalog, metadata);

        loadUniqueConstraints(runner, conn, catalog, metadata);
        loadForeignConstraints(runner, conn, catalog);
    }

    protected abstract void loadForeignConstraints(QueryRunner runner, Connection conn, Catalog catalog) throws SQLException;

    protected abstract void loadUniqueConstraints(QueryRunner runner, Connection conn, Catalog catalog, DatabaseMetaData metadata) throws SQLException;

    protected void loadColumns(Catalog catalog, DatabaseMetaData metadata) {
        catalog.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                try {
                    ResultSet columns = metadata.getColumns(null, table.getSchemaName(), table.getName(), null);
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String columnType = columns.getString("TYPE_NAME");
                        int columnIndex = columns.getInt("ORDINAL_POSITION") - 1;
                        table.addColumn(columnName, columnType, columnIndex);
                    }
                    columns.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }

    protected void loadTables(Catalog catalog, CatalogFilter filter, DatabaseMetaData metadata) {
        catalog.forEachSchema(schema -> {
            try {
                ResultSet tables = metadata.getTables(null, schema.getName(), null, null);
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableType = tables.getString("TABLE_TYPE");

                    if ("TABLE".equals(tableType) && filter.acceptTable(schema.getName(), tableName)) {
                        schema.addTable(tableName);
                    }
                }
                tables.close();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        );
    }

    protected void loadSchemas(Catalog catalog, CatalogFilter filter, DatabaseMetaData metadata) throws SQLException {
        if (isMariaDb(metadata.getDriverName())) {
            try (ResultSet catalogs = metadata.getCatalogs()) {
                while (catalogs.next()) {
                    String schemaName = catalogs.getString("TABLE_CAT");
                    if (filter.acceptSchema(schemaName)) {
                        catalog.addSchema(schemaName);
                    }
                }
            }
        } else {
            try (ResultSet schemas = metadata.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    if (filter.acceptSchema(schemaName)) {
                        catalog.addSchema(schemaName);
                    }
                }
            }
        }
    }

    private boolean isMariaDb(String driverName) {
        return driverName.contains("MariaDB");
    }
}
