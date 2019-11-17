package me.vzhilin.dbrow.adapter;

public interface RowValue {
    @Override
    String toString();

    Object get();
}
