package me.vzhilin.adapter.conv;

import me.vzhilin.adapter.Converter;

public final class TextConverter implements Converter {
    public final static TextConverter INSTANCE = new TextConverter();

    @Override
    public Object conv(String text) {
        return text;
    }
}
