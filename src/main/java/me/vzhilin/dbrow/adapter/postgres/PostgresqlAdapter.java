package me.vzhilin.dbrow.adapter.postgres;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.IdentifierCase;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.catalog.Table;
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
    public String qualifiedTableName(String schemaName, String tableName) {
        return String.format("\"%s\".\"%s\"", schemaName, tableName);
    }

    @Override
    public String qualifiedColumnName(String column) {
        return "\"" + column + "\"";
    }

    @Override
    public String qualifiedTableName(Table table) {
        String schemaName = table.getSchemaName();
        if (schemaName != null) {
            return qualifiedTableName(table.getSchemaName(), table.getName());
        } else {
            return String.format("\"%s\"", table.getName());
        }
    }

    @Override
    public String defaultSchema(Connection conn) throws SQLException {
        return new QueryRunner().query(conn, " select current_schema()", new ScalarHandler<>());
    }

    @Override
    public void dropTables(Connection conn, Iterable<String> tables) throws SQLException {
        QueryRunner runner = new QueryRunner();
        String schemaName = defaultSchema(conn);

        for (String name: tables) {
            try {
                runner.update("DROP TABLE IF EXISTS " + qualifiedTableName(schemaName, name));
            } catch (SQLException ex) {
                // IGNORE
            }
        }
    }

    @Override
    public String qualifiedSchemaName(String schemaName) {
        return "\"" + schemaName + "\"";
    }

    @Override
    public IdentifierCase getDefaultCase() {
        return IdentifierCase.LOWER;
    }
}
