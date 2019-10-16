package me.vzhilin.adapter.conv;

import me.vzhilin.adapter.Converter;

public final class LongConverter implements Converter {
    public static final Converter INSTANCE = new LongConverter();

    @Override
    public Object conv(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
