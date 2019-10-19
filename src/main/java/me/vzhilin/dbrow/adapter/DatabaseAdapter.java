package me.vzhilin.dbrow.adapter;

import me.vzhilin.dbrow.catalog.Table;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseAdapter {
    ValueConverter getConverter();

    String qualifiedTableName(Table table);

    String defaultSchema(Connection conn) throws SQLException;
}
