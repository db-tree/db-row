package me.vzhilin.dbrow.adapter;

import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.TableId;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseAdapter {
    String defaultSchema(Connection conn) throws SQLException;
    void dropTables(Connection conn, Iterable<TableId> tables) throws SQLException;

    IdentifierCase getDefaultCase();
    ValueConverter getConverter();

    String qualifiedTableName(String schemaName, String tableName);

    String qualifiedTableName(Table table);

    String qualifiedColumnName(String column);

    String qualifiedSchemaName(String schemaName);

    ColumnTypeInfo getInfo();

    ValueAccessor getAccessor();
}
