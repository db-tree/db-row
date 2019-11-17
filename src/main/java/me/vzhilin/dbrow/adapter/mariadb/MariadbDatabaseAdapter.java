package me.vzhilin.dbrow.adapter.mariadb;

import me.vzhilin.dbrow.adapter.*;
import me.vzhilin.dbrow.adapter.oracle.OracleValueAccessor;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.TableId;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.SQLException;

public class MariadbDatabaseAdapter implements DatabaseAdapter {
    private final ColumnTypeInfo info = new MariadbColumnTypeInfo();
    private final ValueConverter conv;
    private final ValueAccessor valueAccessor = new OracleValueAccessor();

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
    public void dropTables(Connection conn, Iterable<TableId> tables) throws SQLException {
        QueryRunner runner = new QueryRunner();
        for (TableId id: tables) {
            try {
                runner.update(conn, "DROP TABLE IF EXISTS " + qualifiedTableName(id.getSchemaName(), id.getTableName()));
            } catch (SQLException ex) {
                // IGNORE
            }
        }
    }

    @Override
    public ColumnTypeInfo getInfo() {
        return info;
    }

    @Override
    public ValueAccessor getAccessor() {
        return valueAccessor;
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
