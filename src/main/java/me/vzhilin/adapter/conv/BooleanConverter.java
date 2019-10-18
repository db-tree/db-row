package me.vzhilin.adapter.conv;

import me.vzhilin.adapter.Converter;

public final class BooleanConverter implements Converter {
    public final static BooleanConverter INSTANCE = new BooleanConverter();

    @Override
    public Object conv(String text) {
        if ("true".equals(text)) {
            return true;
        }

        if ("false".equals(text)) {
            return false;
        }
        return null;
    }
}
