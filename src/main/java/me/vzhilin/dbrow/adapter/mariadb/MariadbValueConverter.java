package me.vzhilin.dbrow.adapter.mariadb;

import me.vzhilin.dbrow.adapter.Converter;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.adapter.conv.BigDecimalConverter;
import me.vzhilin.dbrow.adapter.conv.LongConverter;
import me.vzhilin.dbrow.adapter.conv.NeverConverter;
import me.vzhilin.dbrow.adapter.conv.TextConverter;

import java.util.LinkedHashMap;
import java.util.Map;

public final class MariadbValueConverter implements ValueConverter {
    private final Map<String, Converter> converters = new LinkedHashMap<>();

    public MariadbValueConverter() {
        bind(TextConverter.INSTANCE, "CHAR", "VARCHAR", "BINARY", "CHAR BYTE", "VARBINARY", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT");
        bind(BigDecimalConverter.INSTANCE, "BIGINT", "DECIMAL", "DEC", "NUMERIC", "FIXED", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL");
        bind(LongConverter.INSTANCE, "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER");
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
