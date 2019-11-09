package me.vzhilin.dbrow.catalog;

import me.vzhilin.dbrow.util.BiMap;

import java.util.*;

public final class Table {
    private final String name;
    private final Schema schema;
    private final Set<UniqueConstraint> uniqueConstraints = new LinkedHashSet<>();
    private final Set<ForeignKey> foreignKeys = new LinkedHashSet<>();
    private final Map<String, Column> columns = new LinkedHashMap<>();

    public Table(Schema schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    public String getSchemaName() {
        return schema.getName();
    }

    public boolean hasColumn(String columnName) {
        return columns.containsKey(columnName);
    }

    public ForeignKey getForeignKey(String name) {
        return foreignKeys
                .stream()
                .filter(foreignKey -> name.equals(foreignKey.getFkName()))
                .findFirst().orElse(null);
    }

    public boolean hasForeignKey(String name) {
        return foreignKeys.stream().anyMatch(foreignKey -> name.equals(foreignKey.getFkName()));
    }

    public Column addColumn(String columnName, String columnType, int columnIndex) {
        Column column = new Column(this, columnName, columnType, columnIndex);
        columns.put(columnName, column);
        return column;
    }

    public Column getColumn(String columnName) {
        return columns.get(columnName);
    }

//    public PrimaryKeyColumn getPrimaryKeyColumn(String name) {
//        return getPrimaryKey().get().getColumn(name);
//    }

//    public Optional<PrimaryKey> getPrimaryKey() {
//        return primaryKey;
//    }

//    public void setPk(PrimaryKey pk) {
//        primaryKey = Optional.ofNullable(pk);
//    }

//    public ForeignKey addForeignKey(String fkName, Table toTable, BiMap<PrimaryKeyColumn, ForeignKeyColumn> cols) {
//        ForeignKey foreignKey = new ForeignKey(fkName, this, toTable, cols);
//        foreignKeys.put(fkName, foreignKey);
//        return foreignKey;
//    }

    public ForeignKey addForeignKey(String fkName, UniqueConstraint unique, BiMap<UniqueConstraintColumn, Column> mapping) {
        BiMap<UniqueConstraintColumn, ForeignKeyColumn> fkMapping = new BiMap<>();
        ForeignKey fkConstraint = new ForeignKey(fkName, this, unique);
        mapping.forEach((ucc, column) -> fkMapping.put(ucc, new ForeignKeyColumn(fkConstraint, column)));
        fkConstraint.setMapping(fkMapping);
        foreignKeys.add(fkConstraint);
        return fkConstraint;
    }

    public ForeignKey addForeignKey(UniqueConstraint unique, BiMap<UniqueConstraintColumn, Column> mapping) {
        return addForeignKey("", unique, mapping);
    }

    public String getName() {
        return name;
    }

    public Map<String, Column> getColumns() {
        return Collections.unmodifiableMap(columns);
    }

    public Set<ForeignKey> getForeignKeys() {
        return Collections.unmodifiableSet(foreignKeys);
    }

    public int getColumnCount() {
        return columns.size();
    }

    public UniqueConstraint addUniqueConstraint(String constraintName, String... columns) {
        int pos = 1;
        Set<UniqueConstraintColumn> ucc = new LinkedHashSet<>();
        for (String column: columns) {
            ucc.add(new UniqueConstraintColumn(getColumn(column), pos++));
        }
        UniqueConstraint cons = new UniqueConstraint(this, ucc, constraintName);
        uniqueConstraints.add(cons);
        return cons;
    }

    public UniqueConstraint addUniqueConstraint(String... columns) {
        return addUniqueConstraint("", columns);
    }

    public Set<UniqueConstraint> getUniqueConstraints() {
        return Collections.unmodifiableSet(uniqueConstraints);
    }

    public UniqueConstraint getAnyUniqueConstraint() {
        return uniqueConstraints.iterator().next();
    }

    public UniqueConstraint getUniqueConstraint(String ucName) {
        return uniqueConstraints
            .stream()
            .filter(uc -> ucName.equals(uc.getName()))
            .findFirst()
            .get();
    }

    private boolean uniqueConstraintsEquals(Table table) {
        return getUniqueConstraints().equals(table.getUniqueConstraints());
    }

    private boolean foreignKeysEquals(Table table) {
        Set<ForeignKey> fk1 = getForeignKeys();
        Set<ForeignKey> fk2 = table.getForeignKeys();
        return fk1.equals(fk2);
    }

    private boolean columnsEquals(Table table) {
        if (!Objects.equals(getColumnNames(), table.getColumnNames())) {
            return false;
        }

        for (String columnName: getColumnNames()) {
            Column c1 = getColumn(columnName);
            Column c2 = table.getColumn(columnName);

            if (!c1.equals(c2)) {
                return false;
            }
        }
        return true;
    }

    public UniqueConstraint findConstraint(Set<String> uniqueColumns) {
        for (UniqueConstraint uc: uniqueConstraints) {
            if (uniqueColumns.equals(uc.getColumnNames())) {
                return uc;
            }
        }
        return null;
    }

    public Set<String> getColumnNames() {
        return Collections.unmodifiableSet(columns.keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schema);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;

        if (!getSchemaName().equals(table.getSchemaName())) {
            return false;
        }

        if (!name.equals(table.getName())) {
            return false;
        }

        if (!columnsEquals(table)) {
            return false;
        }

        if (!uniqueConstraintsEquals(table)) {
            return false;
        }

        if (!foreignKeysEquals(table)) {
            return false;
        }
        return true;
    }
}
