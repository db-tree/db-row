package me.vzhilin.dbrow.catalog;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.util.BiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

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

    public String getFkAsString() {
        List<String> columns = new ArrayList<>();
        columnMapping.forEach(new BiConsumer<PrimaryKeyColumn, ForeignKeyColumn>() {
            @Override
            public void accept(PrimaryKeyColumn primaryKeyColumn, ForeignKeyColumn foreignKeyColumn) {
                columns.add(foreignKeyColumn.getColumn().getName());
            }
        });
        return Joiner.on(',').join(columns);
    }

    /**
     * @return pkColumn -&gt; fkColumn
     */
    public BiMap<PrimaryKeyColumn, ForeignKeyColumn> getColumnMapping() {
        return columnMapping;
    }

    public int size() {
        return columnMapping.size();
    }

    @Override
    public String toString() {
        return "ForeignKey{" +
                "fkName='" + fkName + '\'' +
                ", table=" + table +
                '}';
    }
}
