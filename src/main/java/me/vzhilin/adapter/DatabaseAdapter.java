package me.vzhilin.adapter;

import me.vzhilin.catalog.Table;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;

public interface DatabaseAdapter {
    JDBCType getType(String typeName);

    ValueConverter getConverter(JDBCType type);

    String qualifiedTableName(Table table);

    String defaultSchema(Connection conn) throws SQLException;
}
