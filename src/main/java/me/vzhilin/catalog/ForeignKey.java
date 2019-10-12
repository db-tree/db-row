package me.vzhilin.catalog;

import me.vzhilin.util.BiMap;

public final class ForeignKey {
    private final String fkName;
    private final Table table;
    private final Table toTable;
    private final BiMap<PrimaryKeyColumn, ForeignKeyColumn> columnMapping;

    public ForeignKey(String fkName, Table table, Table toTable, BiMap<PrimaryKeyColumn, ForeignKeyColumn> columnMapping) {
        this.fkName = fkName;
        this.table = table;
        this.toTable = toTable;
        this.columnMapping = columnMapping;
    }

    public String getFkName() {
        return fkName;
    }

    public Table getTable() {
        return table;
    }

    public Table getPkTable() {
        return toTable;
    }

    /**
     * @return pkColumn -&gt; fkColumn
     */
    public BiMap<PrimaryKeyColumn, ForeignKeyColumn> getColumnMapping() {
        return columnMapping;
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
                "fkName='" + fkName + '\'' +
                ", table=" + table +
                '}';
    }
}
