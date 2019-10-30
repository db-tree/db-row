package me.vzhilin.dbrow.catalog.loader;

import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;

public final class MariaDBCatalogLoader extends MetadataCatalogLoader {
    @Override
    protected void loadForeignConstraints(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter) throws SQLException {
        String sql = "select * from information_schema.KEY_COLUMN_USAGE\n" +
                "where REFERENCED_TABLE_NAME is not null;";

        // foreignKey -> unique_mapping
        Map<ForeignConstraintKey, BiMap<String, String>> mp = new LinkedHashMap<>();

        for (Map<String, Object> m: runner.query(conn, sql, new MapListHandler())) {
            String consSchema = (String) m.get("CONSTRAINT_SCHEMA");
            String consName = (String) m.get("CONSTRAINT_NAME");
            String tableSchema = (String) m.get("TABLE_SCHEMA");
            String tableName = (String) m.get("TABLE_NAME");
            String columnName = (String) m.get("COLUMN_NAME");
            String refSchemaName = (String) m.get("REFERENCED_TABLE_SCHEMA");
            String refTableName = (String) m.get("REFERENCED_TABLE_NAME");
            String refColumnName = (String) m.get("REFERENCED_COLUMN_NAME");

            if (filter.acceptTable(tableSchema, tableName)) {
                ConstraintKey ck = new ConstraintKey(consName, tableSchema, tableName);
                mp.computeIfAbsent(new ForeignConstraintKey(ck, refSchemaName, refTableName), fk -> new BiMap<>())
                        .put(refColumnName, columnName);
            }
        }

        mp.forEach(new BiConsumer<ForeignConstraintKey, BiMap<String, String>>() {
            @Override
            public void accept(ForeignConstraintKey ck, BiMap<String, String> uniqueToFk) {
                Set<String> uniqueColumns = new HashSet<>();
                uniqueToFk.forEach((uniqueColumn, fkColumn) -> uniqueColumns.add(uniqueColumn));

                Table ucTable = catalog.getTable(ck.referencedSchema, ck.referencedTable);
                UniqueConstraint uc = ucTable.findConstraint(uniqueColumns);
                // TODO check not null

                Table table = catalog.getTable(ck.fkCons.tableSchema, ck.fkCons.tableName);
                BiMap<UniqueConstraintColumn, Column> m = new BiMap<UniqueConstraintColumn, Column>();
                uniqueToFk.forEach((ucn, columnName) -> m.put(uc.getColumn(ucn), table.getColumn(columnName)));

                ForeignKey fk = table.addForeignKey(ck.fkCons.consName, uc, m);
                uc.addForeignKey(fk);
            }
        });

        System.err.println(1);
    }

    @Override
    protected void loadUniqueConstraints(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter, DatabaseMetaData metadata) throws SQLException {
        Map<ConstraintKey, Set<String>> keys = new LinkedHashMap<>();

        String sql = "select * from " +
                "information_schema.TABLE_CONSTRAINTS tc " +
                "join information_schema.KEY_COLUMN_USAGE kku on tc.CONSTRAINT_SCHEMA = kku.CONSTRAINT_SCHEMA " +
                "and tc.TABLE_NAME = kku.TABLE_NAME " +
                "and tc.CONSTRAINT_NAME = kku.CONSTRAINT_NAME " +
                "where " +
                "kku.REFERENCED_TABLE_NAME is null " +
                "and " +
                "tc.CONSTRAINT_TYPE in('UNIQUE', 'PRIMARY KEY')";
        for (Map<String, Object> m: runner.query(conn, sql, new MapListHandler())) {
            String consName = (String) m.get("CONSTRAINT_NAME");
            String tableSchema = (String) m.get("TABLE_SCHEMA");
            String tableName = (String) m.get("TABLE_NAME");
            String columnName = (String) m.get("COLUMN_NAME");

            if (filter.acceptTable(tableSchema, tableName)) {
                ConstraintKey key = new ConstraintKey(consName, tableSchema, tableName);
                keys.computeIfAbsent(key, ck -> new LinkedHashSet<>()).add(columnName);
            }

        }

        keys.forEach((ck, ss) -> {
            Table tb = catalog.getTable(ck.tableSchema, ck.tableName);
            tb.addUniqueConstraint(ck.consName, ss.toArray(new String[0]));
        });
    }

    private final static class ForeignConstraintKey {
        private final ConstraintKey fkCons;
        private final String referencedSchema;
        private final String referencedTable;

        private ForeignConstraintKey(ConstraintKey fkCons, String referencedSchema, String referencedTable) {
            this.fkCons = fkCons;
            this.referencedSchema = referencedSchema;
            this.referencedTable = referencedTable;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForeignConstraintKey that = (ForeignConstraintKey) o;
            return fkCons.equals(that.fkCons) &&
                    referencedSchema.equals(that.referencedSchema) &&
                    referencedTable.equals(that.referencedTable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fkCons, referencedSchema, referencedTable);
        }
    }

    private final static class ConstraintKey {
        private final String consName;
        private final String tableSchema;
        private final String tableName;

        private ConstraintKey(String consName, String tableSchema, String tableName) {
            this.consName = consName;
            this.tableSchema = tableSchema;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConstraintKey that = (ConstraintKey) o;
            return consName.equals(that.consName) &&
                    tableSchema.equals(that.tableSchema) &&
                    tableName.equals(that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(consName, tableSchema, tableName);
        }
    }
}
