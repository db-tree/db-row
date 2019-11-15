package me.vzhilin.dbrow.catalog;

import java.util.Objects;

public final class TableId {
    private final String schemaName;
    private final String tableName;

    public TableId(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableId tableId = (TableId) o;
        return schemaName.equals(tableId.schemaName) &&
                tableName.equals(tableId.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }
}
