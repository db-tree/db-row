package me.vzhilin.dbrow.adapter.conv;

import me.vzhilin.dbrow.adapter.Converter;

import java.util.UUID;

public final class UUIDConverter implements Converter {
    public static final Converter INSTANCE = new UUIDConverter();

    @Override
    public Object fromString(String text) {
        try {
            return UUID.fromString(text);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    @Override
    public String toString(Object o) {
        return String.valueOf(o);
    }
}
