package me.vzhilin.dbrow.catalog.loader;

import com.google.common.base.Joiner;
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
    protected void loadTables(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter) throws SQLException {
        String sql = "select TABLE_SCHEMA, TABLE_NAME from information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
        for (Map<String, Object> m: runner.query(conn, sql, new MapListHandler())) {
            String tableSchema = (String) m.get("TABLE_SCHEMA");
            String tableName = (String) m.get("TABLE_NAME");

            if (filter.acceptTable(tableSchema, tableName)) {
                catalog.addSchema(tableSchema).addTable(tableName);
            }
        }
    }

    @Override
    protected void loadColumns(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter) throws SQLException {
        for (String schema: catalog.getSchemaNames()) {
            Set<String> tableNames = catalog.getSchema(schema).getTableNames();

            Character[] qms = new Character[tableNames.size()];
            Arrays.fill(qms, '?');
            String qm = Joiner.on(',').join(qms);

            List<Object> params = new ArrayList<>(tableNames.size() + 1);
            params.add(schema);
            params.addAll(tableNames);

            String sql = "select TABLE_NAME, COLUMN_NAME, COLUMN_TYPE from information_schema.`COLUMNS`" +
                    " where TABLE_SCHEMA = ? AND TABLE_NAME in (" + qm + ")";

            for (Map<String, Object> m: runner.query(conn, sql, new MapListHandler(), params.toArray())) {
                String tableName = (String) m.get("TABLE_NAME");
                String columnName = (String) m.get("COLUMN_NAME");
                String columnType = (String) m.get("COLUMN_TYPE");

                if (filter.acceptColumn(schema, tableName, columnName)) {
                    String type;
                    Integer length = null;
                    Integer precision = null;
                    int br = columnType.indexOf('(');
                    if (br != -1) {
                        type = columnType.substring(0, br);
                        int comma = columnType.indexOf(',');
                        if (comma != -1) {
                            length = Integer.parseInt(columnType.substring(br + 1, comma));
                            precision = Integer.parseInt(columnType.substring(comma + 1, columnType.length() - 1));
                        } else {
                            length = Integer.parseInt(columnType.substring(br, columnType.length() - 1));
                        }
                    } else {
                        type = columnType;
                    }
                    Column column = catalog.getSchema(schema).addTable(tableName).addColumn(columnName, type);
                    column.setLength(length);
                    column.setPrecision(precision);
                }
            }
        }

    }

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
            UniqueConstraint uc = tb.addUniqueConstraint(ck.consName, ss.toArray(new String[0]));
            ss.forEach(ucName -> tb.getColumn(ucName).addUniqueConstraint(uc));
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
