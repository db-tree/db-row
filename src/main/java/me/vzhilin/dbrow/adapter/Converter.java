package me.vzhilin.dbrow.adapter;

public interface Converter {
    Object fromString(String text);
    String toString(Object o);
}
