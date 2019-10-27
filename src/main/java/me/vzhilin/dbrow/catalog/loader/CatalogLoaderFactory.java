package me.vzhilin.dbrow.catalog.loader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CatalogLoaderFactory {
    private Map<String, CatalogLoader> registeredLoaders = new HashMap<>();

    public CatalogLoaderFactory() {
        registeredLoaders.put("Oracle JDBC driver", new OracleCatalogLoader());
    }

    public CatalogLoader getLoader(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            String driverName = getDriverName(conn);
            CatalogLoader loader = registeredLoaders.get(driverName);
            // TODO check if not null
            return loader;
        }
    }

    private String getDriverName(Connection conn) {
        try {
            return conn.getMetaData().getDriverName();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
