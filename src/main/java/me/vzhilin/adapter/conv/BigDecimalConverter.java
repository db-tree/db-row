package me.vzhilin.adapter.conv;

import me.vzhilin.adapter.Converter;

import java.math.BigDecimal;

public final class BigDecimalConverter implements Converter {
    public static final Converter INSTANCE = new BigDecimalConverter();

    @Override
    public Object conv(String text) {
        try {
            return new BigDecimal(text);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
