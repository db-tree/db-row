package me.vzhilin.dbrow.adapter.oracle;

import me.vzhilin.dbrow.adapter.Converter;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.adapter.conv.BigDecimalConverter;
import me.vzhilin.dbrow.adapter.conv.LongConverter;
import me.vzhilin.dbrow.adapter.conv.NeverConverter;
import me.vzhilin.dbrow.adapter.conv.TextConverter;

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
    public Object fromString(String text, String dataType) {
        return converters.getOrDefault(dataType, NeverConverter.INSTANCE).fromString(text);
    }
}
