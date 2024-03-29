package me.vzhilin.dbrow.catalog;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class Column {
    private final String name;
    private final String dataType;
    private final Table table;
    private final int index;
    private Integer length;
    private Integer precision;
    private final Set<UniqueConstraint> uniqueConstraints = new LinkedHashSet<>();
    private final Set<ForeignKey> foreignKeys = new LinkedHashSet<>();

    public Column(Table table, String name, String dataType, int index) {
        this.table = table;
        this.name = name;
        this.dataType = dataType;
        this.index = index;
    }

    public String getSchema() {
        return table.getSchemaName();
    }

    public String getTableName() {
        return table.getName();
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public void addForeignKey(ForeignKey fk) {
        foreignKeys.add(fk);
    }

    public void addUniqueConstraint(UniqueConstraint uc) {
        uniqueConstraints.add(uc);
    }

    public Set<UniqueConstraint> getUniqueConstraints() {
        return Collections.unmodifiableSet(uniqueConstraints);
    }

    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(foreignKeys);
    }

    public int getIndex() {
        return index;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    @Override
    public String toString() {
        return table.getSchemaName() + "." + table.getName() + "." + name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return index == column.index &&
                name.equals(column.name) &&
                dataType.equals(column.dataType) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, index);
    }
}
