package me.vzhilin.dbrow.adapter.postgres;

import me.vzhilin.dbrow.adapter.ColumnTypeInfo;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.IdentifierCase;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.TableId;
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
    public void dropTables(Connection conn, Iterable<TableId> tables) throws SQLException {
        QueryRunner runner = new QueryRunner();
        for (TableId id: tables) {
            try {
                runner.update(conn, "DROP TABLE IF EXISTS " + qualifiedTableName(id.getSchemaName(), id.getTableName()));
            } catch (SQLException ex) {
                ex.printStackTrace(System.err);
            }
        }
    }

    @Override
    public String qualifiedSchemaName(String schemaName) {
        return "\"" + schemaName + "\"";
    }

    @Override
    public ColumnTypeInfo getInfo() {
        return null;
    }

    @Override
    public IdentifierCase getDefaultCase() {
        return IdentifierCase.LOWER;
    }
}
