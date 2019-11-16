package me.vzhilin.dbrow.adapter.postgres;

import me.vzhilin.dbrow.adapter.Converter;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.adapter.conv.*;

import java.util.LinkedHashMap;
import java.util.Map;

public final class PostgresqlValueConverter implements ValueConverter {
    private final Map<String, Converter> converters = new LinkedHashMap<>();

    public PostgresqlValueConverter() {
        bind(TextConverter.INSTANCE, "bpchar", "text", "varchar", "char", "UUID");
        bind(BigDecimalConverter.INSTANCE, "decimal", "numeric", "float", "float4", "float8", "real", "double precision", "bigserial", "money");
        bind(LongConverter.INSTANCE, "smallint", "integer", "bigint", "serial", "int2", "int4", "int8");
        bind(BooleanConverter.INSTANCE, "boolean", "bool");
        bind(UUIDConverter.INSTANCE, "uuid");
    }

    private void bind(Converter conv, String... dataTypes) {
        for (String type: dataTypes) {
            converters.put(type, conv);
        }
    }

    @Override
    public Object fromString(String text, String dataType) {
        return converters.getOrDefault(dataType, NeverConverter.INSTANCE).fromString(text);
    }
}
