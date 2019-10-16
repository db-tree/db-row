package me.vzhilin.adapter.postgres;

import me.vzhilin.adapter.Converter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.adapter.conv.BigDecimalConverter;
import me.vzhilin.adapter.conv.LongConverter;
import me.vzhilin.adapter.conv.NeverConverter;
import me.vzhilin.adapter.conv.TextConverter;

import java.util.LinkedHashMap;
import java.util.Map;

public final class PostgresqlValueConverter implements ValueConverter {
    private final Map<String, Converter> converters = new LinkedHashMap<>();

    public PostgresqlValueConverter() {
        bind(TextConverter.INSTANCE, "bpchar", "text", "varchar", "char", "UUID");
        bind(BigDecimalConverter.INSTANCE, "decimal", "numeric", "float", "float4", "float8", "real", "double precision", "bigserial", "money");
        bind(LongConverter.INSTANCE, "smallint", "integer", "bigint", "serial", "int2", "int4", "int8");
    }

    private void bind(Converter conv, String... dataTypes) {
        for (String type: dataTypes) {
            converters.put(type, conv);
        }
    }

    @Override
    public Object fromString(String text, String dataType) {
        return converters.getOrDefault(dataType, NeverConverter.INSTANCE).conv(text);
    }
}
