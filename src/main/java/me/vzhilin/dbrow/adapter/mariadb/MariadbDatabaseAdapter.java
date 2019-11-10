package me.vzhilin.dbrow.adapter.mariadb;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.IdentifierCase;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.catalog.Table;

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
    public String qualifiedTableName(String schemaName, String tableName) {
        return String.format("`%s`.`%s`", schemaName, tableName);
    }

    @Override
    public String qualifiedColumnName(String column) {
        return "`" + column + "`";
    }

    @Override
    public String qualifiedTableName(Table table) {
        String schemaName = table.getSchemaName();
        if (schemaName != null) {
            return qualifiedTableName(table.getSchemaName(), table.getName());
        } else {
            return "`" + table.getName() + "`";
        }
    }

    @Override
    public String defaultSchema(Connection conn) throws SQLException {
        return conn.getCatalog();
    }

    @Override
    public String qualifiedSchemaName(String schemaName) {
        return "`" + schemaName + "`";
    }

    @Override
    public IdentifierCase getDefaultCase() {
        return IdentifierCase.LOWER;
    }
}
