package me.vzhilin.dbrow.adapter.conv;

import me.vzhilin.dbrow.adapter.Converter;

import java.math.BigDecimal;

public final class BigDecimalConverter implements Converter {
    public static final Converter INSTANCE = new BigDecimalConverter();

    @Override
    public Object fromString(String text) {
        try {
            return new BigDecimal(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public String toString(Object o) {
        return String.valueOf(o);
    }
}
