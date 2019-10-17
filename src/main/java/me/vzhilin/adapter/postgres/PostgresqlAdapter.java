package me.vzhilin.adapter.postgres;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.Table;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;

public final class PostgresqlAdapter implements DatabaseAdapter {
    private final ValueConverter conv;

    public PostgresqlAdapter() {
        this.conv = new PostgresqlValueConverter();
    }

    @Override
    public ValueConverter getConverter() {
        return conv;
    }

    @Override
    public String qualifiedTableName(Table table) {
        String schemaName = table.getSchemaName();
        if (schemaName != null) {
            return String.format("\"%s\".\"%s\"", schemaName, table.getName());
        } else {
            return String.format("\"%s\"", table.getName());
        }
    }

    @Override
    public String defaultSchema(Connection conn) throws SQLException {
        return new QueryRunner().query(conn, " select current_schema()", new ScalarHandler<>());
    }
}
