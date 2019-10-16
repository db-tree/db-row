package me.vzhilin.adapter;

import me.vzhilin.catalog.Table;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseAdapter {
    ValueConverter getConverter();

    String qualifiedTableName(Table table);

    String defaultSchema(Connection conn) throws SQLException;
}
