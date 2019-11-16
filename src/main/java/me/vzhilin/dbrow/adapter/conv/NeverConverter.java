package me.vzhilin.dbrow.adapter.conv;

import me.vzhilin.dbrow.adapter.Converter;

public final class NeverConverter implements Converter {
    public final static Converter INSTANCE = new NeverConverter();

    @Override
    public Object fromString(String text) {
        return null;
    }

    @Override
    public String toString(Object o) {
        return String.valueOf(o);
    }
}
