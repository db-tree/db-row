package me.vzhilin.dbrow.adapter;

import me.vzhilin.dbrow.adapter.conv.BigDecimalConverter;
import me.vzhilin.dbrow.adapter.conv.LongConverter;
import me.vzhilin.dbrow.adapter.conv.NeverConverter;
import me.vzhilin.dbrow.adapter.conv.TextConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BasicColumnTypeInfo implements ColumnTypeInfo {
    private final Map<String, ColumnTypeDescription> columnTypes = new HashMap<>();

    protected void addColumnType(ColumnTypeDescription description) {
        columnTypes.put(description.getName().toLowerCase(), description);
    }

    protected ColumnTypeDescription addDecimal(String name) {
        ColumnTypeDescription desc = new ColumnTypeDescription(name, ColumnType.DECIMAL);
        desc.setConv(BigDecimalConverter.INSTANCE);
        return desc;
    }

    protected ColumnTypeDescription addFloat(String name) {
        ColumnTypeDescription desc = new ColumnTypeDescription(name, ColumnType.FLOAT);
        desc.setConv(BigDecimalConverter.INSTANCE); // FIXME DOUBLE
        return desc;
    }

    protected ColumnTypeDescription addInteger(String name) {
        ColumnTypeDescription desc = new ColumnTypeDescription(name, ColumnType.INTEGER);
        desc.setConv(LongConverter.INSTANCE);
        return desc;
    }

    protected ColumnTypeDescription addString(String name) {
        ColumnTypeDescription desc = new ColumnTypeDescription(name, ColumnType.STRING);
        desc.setConv(TextConverter.INSTANCE);
        return desc;
    }

    protected ColumnTypeDescription addDate(String name) {
        ColumnTypeDescription desc = new ColumnTypeDescription(name, ColumnType.DATE);
        desc.setConv(NeverConverter.INSTANCE);
        return desc;
    }

    @Override
    public ColumnTypeDescription getType(String type) {
        ColumnTypeDescription desc = columnTypes.get(type.toLowerCase());
        if (desc == null) {
            return new ColumnTypeDescription("unknown", ColumnType.UNKNOWN);
        }
        return desc;
    }

    @Override
    public Map<String, ColumnTypeDescription> getColumnTypes() {
        return Collections.unmodifiableMap(columnTypes);
    }
}
