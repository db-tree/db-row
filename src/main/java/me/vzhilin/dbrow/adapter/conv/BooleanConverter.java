package me.vzhilin.dbrow.adapter.conv;

import me.vzhilin.dbrow.adapter.Converter;

public final class BooleanConverter implements Converter {
    public final static BooleanConverter INSTANCE = new BooleanConverter();

    @Override
    public Object fromString(String text) {
        if ("true".equals(text)) {
            return true;
        }

        if ("false".equals(text)) {
            return false;
        }
        return null;
    }

    @Override
    public String toString(Object o) {
        return String.valueOf(o);
    }
}
