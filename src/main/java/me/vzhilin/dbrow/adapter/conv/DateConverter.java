package me.vzhilin.dbrow.adapter.conv;

import me.vzhilin.dbrow.adapter.Converter;

import java.time.Instant;
import java.util.Date;

public final class DateConverter implements Converter  {
    public static final Converter INSTANCE = new DateConverter();

    @Override
    public Object fromString(String text) {
        return Date.from(Instant.parse(text));
    }

    @Override
    public String toString(Object o) {
        return String.valueOf(o);
    }
}
