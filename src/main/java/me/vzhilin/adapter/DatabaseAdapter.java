package me.vzhilin.adapter;

import me.vzhilin.catalog.Table;

import java.sql.JDBCType;

public interface DatabaseAdapter {
    JDBCType getType(String typeName);

    ValueConverter getConverter(JDBCType type);

    String qualifiedTableName(Table table);
}
