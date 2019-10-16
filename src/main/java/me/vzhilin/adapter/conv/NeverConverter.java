package me.vzhilin.adapter.conv;

import me.vzhilin.adapter.Converter;

public final class NeverConverter implements Converter {
    public final static Converter INSTANCE = new NeverConverter();

    @Override
    public Object conv(String text) {
        return null;
    }
}
