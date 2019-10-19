package me.vzhilin.dbrow.adapter;

public interface ValueConverter {
    Object fromString(String value, String dataType);
}
