package me.vzhilin.adapter.oracle;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.Table;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.SQLException;

public class OracleDatabaseAdapter implements DatabaseAdapter {
    private final OracleValueConverter conv;

    public OracleDatabaseAdapter() {
        this.conv = new OracleValueConverter();
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
        return new QueryRunner().query(conn,"select sys_context('userenv', 'current_schema') from dual", new ScalarHandler<>());
    }
}
