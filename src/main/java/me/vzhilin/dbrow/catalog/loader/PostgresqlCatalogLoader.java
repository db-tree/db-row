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

public final class PostgresqlCatalogLoader extends MetadataCatalogLoader {
    @Override
    protected void loadForeignConstraints(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter) throws SQLException {
        String sql = "select " +
                "kcu_uc.table_schema as uc_schema, " +
                "kcu_uc.table_name as uc_table, " +
                "kcu_uc.constraint_name as uc_name, " +
                "kcu_uc.column_name as uc_column, " +
                "kcu_fk.table_schema as fk_schema, " +
                "kcu_fk.table_name as fk_table, " +
                "kcu_fk.constraint_name as fk_name, " +
                "kcu_fk.column_name as fk_column " +
                " from  " +
                "information_schema.KEY_COLUMN_USAGE kcu_uc " +
                "join " +
                "information_schema.referential_constraints ref_cons on  " +
                "kcu_uc.constraint_name = ref_cons.unique_constraint_name and " +
                "kcu_uc.constraint_schema = ref_cons.unique_constraint_schema and " +
                "kcu_uc.constraint_catalog = ref_cons.unique_constraint_catalog " +
                "join " +
                "information_schema.KEY_COLUMN_USAGE kcu_fk on kcu_uc.ordinal_position = kcu_fk.ordinal_position " +
                "and kcu_fk.constraint_name = ref_cons.constraint_name " +
                "and kcu_fk.constraint_schema = ref_cons.constraint_schema " +
                "and kcu_fk.constraint_catalog = ref_cons.constraint_catalog";

        // foreignKey -> unique_mapping
        Map<ForeignConstraintKey, BiMap<String, String>> mp = new LinkedHashMap<>();

        for (Map<String, Object> m: runner.query(conn, sql, new MapListHandler())) {
            String ucName = (String) m.get("uc_name");
            String ucSchema = (String) m.get("uc_schema");
            String ucTable = (String) m.get("uc_table");
            String ucColumnName = (String) m.get("uc_column");
            String refSchemaName = (String) m.get("fk_schema");
            String refName = (String) m.get("fk_name");
            String refTableName = (String) m.get("fk_table");
            String refColumnName = (String) m.get("fk_column");

            if (filter.acceptTable(ucSchema, ucTable)) {
                ConstraintKey ck = new ConstraintKey(refName, refSchemaName, refTableName);
                ForeignConstraintKey fck = new ForeignConstraintKey(ck, ucSchema, ucTable, ucName);
                mp.computeIfAbsent(fck, fk -> new BiMap<>()).put(ucColumnName, refColumnName);
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
                BiMap<UniqueConstraintColumn, Column> m = new BiMap<>();
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
        private final String ucName;

        private ForeignConstraintKey(ConstraintKey fkCons, String ucSchema, String ucTable, String ucName) {
            this.fkCons = fkCons;
            this.referencedSchema = ucSchema;
            this.referencedTable = ucTable;
            this.ucName = ucName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForeignConstraintKey that = (ForeignConstraintKey) o;
            return fkCons.equals(that.fkCons) &&
                    referencedSchema.equals(that.referencedSchema) &&
                    referencedTable.equals(that.referencedTable) &&
                    ucName.equals(that.ucName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fkCons, referencedSchema, referencedTable, ucName);
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
