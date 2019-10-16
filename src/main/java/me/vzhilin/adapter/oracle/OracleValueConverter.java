package me.vzhilin.adapter.oracle;

import me.vzhilin.adapter.Converter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.adapter.conv.BigDecimalConverter;
import me.vzhilin.adapter.conv.LongConverter;
import me.vzhilin.adapter.conv.NeverConverter;
import me.vzhilin.adapter.conv.TextConverter;

import java.util.LinkedHashMap;
import java.util.Map;

public final class OracleValueConverter implements ValueConverter {
    private final Map<String, Converter> converters = new LinkedHashMap<>();

    public OracleValueConverter() {
        bind(TextConverter.INSTANCE, "VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2");
        bind(BigDecimalConverter.INSTANCE, "NUMBER", "FLOAT", "DOUBLE", "DECIMAL");
        bind(LongConverter.INSTANCE, "INTEGER");
    }

    private void bind(Converter conv, String... dataTypes) {
        for (String type: dataTypes) {
            converters.put(type, conv);
        }
    }

    @Override
    public Object fromString(String value, String dataType) {
        return converters.getOrDefault(value, NeverConverter.INSTANCE);
    }
}
