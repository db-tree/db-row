package me.vzhilin.adapter.conv;

import me.vzhilin.adapter.Converter;

import java.util.UUID;

public final class UUIDConverter implements Converter {
    public static final Converter INSTANCE = new UUIDConverter();

    @Override
    public Object conv(String text) {
        try {
            return UUID.fromString(text);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
