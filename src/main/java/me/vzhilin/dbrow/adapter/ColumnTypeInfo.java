package me.vzhilin.dbrow.adapter;

import java.util.Map;

public interface ColumnTypeInfo {
    ColumnTypeDescription getType(String type);
    Map<String, ColumnTypeDescription> getColumnTypes();
}
