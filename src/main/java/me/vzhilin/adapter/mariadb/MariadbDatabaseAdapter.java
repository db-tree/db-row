package me.vzhilin.adapter.mariadb;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.Table;

import java.sql.Connection;
import java.sql.SQLException;

public class MariadbDatabaseAdapter implements DatabaseAdapter {
    private final ValueConverter conv;

    public MariadbDatabaseAdapter() {
        conv = new MariadbValueConverter();
    }

    @Override
    public ValueConverter getConverter() {
        return conv;
    }

    @Override
    public String qualifiedTableName(Table table) {
        String schemaName = table.getSchemaName();
        if (schemaName != null) {
            return String.format("`%s`.`%s`", schemaName, table.getName());
        } else {
            return String.format("`%s`", table.getName());
        }
    }

    @Override
    public String defaultSchema(Connection conn) throws SQLException {
        return conn.getCatalog();
    }
}
