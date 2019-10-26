package me.vzhilin.dbrow.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.catalog.filter.AcceptAny;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class CatalogLoader {
    private final DatabaseAdapter adapter;

    public CatalogLoader(DatabaseAdapter adapter) {
        this.adapter = adapter;
    }

    public Catalog load(DataSource ds) throws SQLException {
        return load(ds, new AcceptAny());
    }

    public Catalog load(DataSource ds, CatalogFilter filter) throws SQLException {
        Catalog catalog = new Catalog();
        Connection conn = ds.getConnection();
        DatabaseMetaData metadata = conn.getMetaData();
        loadTables(catalog, filter, metadata);
        conn.close();
        return catalog;
    }

    private void loadTables(Catalog catalog, CatalogFilter filter, DatabaseMetaData metadata) throws SQLException {
    }

    private boolean isMariaDb(String driverName) {
        return driverName.contains("MariaDB");
    }
}
