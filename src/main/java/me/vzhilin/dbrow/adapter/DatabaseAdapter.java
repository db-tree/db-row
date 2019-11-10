package me.vzhilin.dbrow.adapter;

import me.vzhilin.dbrow.catalog.Table;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseAdapter {
    String defaultSchema(Connection conn) throws SQLException;
    void dropTables(Connection conn, Iterable<String> tables) throws SQLException;

    IdentifierCase getDefaultCase();
    ValueConverter getConverter();

    String qualifiedTableName(String schemaName, String tableName);

    String qualifiedTableName(Table table);

    String qualifiedColumnName(String column);

    String qualifiedSchemaName(String schemaName);
}
