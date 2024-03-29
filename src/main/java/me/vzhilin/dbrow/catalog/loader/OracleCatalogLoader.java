package me.vzhilin.dbrow.catalog.loader;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;

public final class OracleCatalogLoader extends MetadataCatalogLoader {
    public static final int ORACLE_LIMIT = 1000;

    @Override
    protected void loadUniqueConstraints(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter, DatabaseMetaData metadata) throws SQLException {
        for (Schema schema: catalog.getSchemas()) {
            for (List<String> tableNames: Iterables.partition(schema.getTableNames(), ORACLE_LIMIT)) {
                List<Object> params = new ArrayList<>(tableNames.size() + 1);
                params.add(schema.getName());
                params.addAll(tableNames);

                List<Map<String, Object>> rs = runner.query(conn, buildUniqueConsQuery(tableNames), new MapListHandler(), params.toArray());

                Map<String, Table> uniqueConstraintTable = new HashMap<>();
                Multimap<String, UniqueConstraintColumn> uniqueConstraintColumns = HashMultimap.create();
                for (Map<String, Object> m: rs) {
                    String owner = (String) m.get("OWNER");
                    String constraintName = (String) m.get("CONSTRAINT_NAME");
                    String constraintType = (String) m.get("CONSTRAINT_TYPE");
                    String tableName = (String) m.get("TABLE_NAME");
                    String columnName = (String) m.get("COLUMN_NAME");
                    int position = ((Number) m.get("POSITION")).intValue();

                    Schema s = catalog.getSchema(owner);
                    Table t = s.getTable(tableName);
                    Column c = t.getColumn(columnName);

                    UniqueConstraintColumn ucc = new UniqueConstraintColumn(c, position);
                    uniqueConstraintColumns.put(constraintName, ucc);

                    uniqueConstraintTable.put(constraintName, t);
                }
                uniqueConstraintColumns.asMap().forEach((consName, uccs) -> {
                    List<UniqueConstraintColumn> arr = new ArrayList<>(uccs);
                    String[] columns = arr.stream()
                            .sorted(Comparator.comparingInt(UniqueConstraintColumn::getPosition))
                            .map(UniqueConstraintColumn::getName)
                            .toArray(String[]::new);

                    Table table = uniqueConstraintTable.get(consName);
                    UniqueConstraint uniq = table.addUniqueConstraint(consName, columns);
                    for (String column : columns) {
                        table.getColumn(column).addUniqueConstraint(uniq);
                    }
                });
            }
        }
    }

    private String buildUniqueConsQuery(List<String> tableNames) {
        Character[] qms = new Character[tableNames.size()];
        Arrays.fill(qms, '?');
        String qm = Joiner.on(',').join(qms);

        return "select\n" +
                "    uc.owner,\n" +
                "    uc.constraint_name, \n" +
                "    uc.constraint_type, \n" +
                "    uc.table_name, \n" +
                "    ucc.column_name, \n" +
                "    ucc.position \n" +
                "from all_constraints uc \n" +
                "join all_cons_columns ucc on uc.constraint_name = ucc.constraint_name\n" +
                "where uc.constraint_type in ('U', 'P') AND uc.owner = ? AND uc.table_name IN (" + qm + ")";
    }

    @Override
    protected void loadForeignConstraints(QueryRunner runner, Connection conn, Catalog catalog, CatalogFilter filter) throws SQLException {
        for (Schema schema: catalog.getSchemas()) {
            for (List<String> tableNames : Iterables.partition(schema.getTableNames(), ORACLE_LIMIT)) {
                Character[] qms = new Character[tableNames.size()];
                Arrays.fill(qms, '?');
                String qm = Joiner.on(',').join(qms);

                List<Object> params = new ArrayList<>(tableNames.size() + 1);
                params.add(schema.getName());
                params.addAll(tableNames);

                Map<String, BiMap<UniqueConstraintColumn, Column>> fkNameToColumnMapping = new HashMap<>();
                String columnQuery =
                        "select \n" +
                        "    uc.owner, \n" +
                        "    uc.table_name, \n" +
                        "    uc.constraint_name as fk_constraint, \n" +
                        "    uc.r_owner as uc_owner, \n" +
                        "    uc.r_constraint_name as uc_constraint,\n" +
                        "    fk.column_name as fk_column, \n" +
                        "    fk.position as fk_position, \n" +
                        "    uc.column_name as uc_column, \n" +
                        "    uc.position as uc_position\n" +
                        "from all_constraints uc \n" +
                        "join all_cons_columns fk on fk.constraint_name = uc.constraint_name\n" +
                        "join all_cons_columns uc on uc.constraint_name = uc.r_constraint_name\n" +
                        "where uc.constraint_type in ('R')\n" +
                        "and uc.owner = ? and uc.table_name in (" + qm + ") " +
                        "and fk.position = uc.position";

                for (Map<String, Object> m: runner.query(conn, columnQuery, new MapListHandler(), params.toArray())) {
                    String fkOwner = (String) m.get("OWNER");
                    String fkTable = (String) m.get("TABLE_NAME");
                    String fkConstraint = (String) m.get("FK_CONSTRAINT");

                    int ucPos = ((Number) m.get("UC_POSITION")).intValue();
                    String ucOwner = (String) m.get("UC_OWNER");
                    String ucConstraint = (String) m.get("UC_CONSTRAINT");

                    int fkPos = ((Number) m.get("FK_POSITION")).intValue();
                    String fkColumn = (String) m.get("FK_COLUMN");
                    String ucColumn = (String) m.get("UC_COLUMN");

                    Table fkT = catalog.getSchema(fkOwner).getTable(fkTable);
                    UniqueConstraint ucc = catalog.getSchema(ucOwner).getUniqueConstraint(ucConstraint);

                    fkNameToColumnMapping
                        .computeIfAbsent(fkConstraint, s -> new BiMap<>())
                        .put(ucc.getColumn(ucColumn), fkT.getColumn(fkColumn));
                }

                String query =
                    "select owner, table_name, constraint_name, r_owner, r_constraint_name\n" +
                    "from all_constraints uc \n" +
                    "where constraint_type in ('R') and owner = ? and table_name in (" + qm + ") ";

                List<Map<String, Object>> rs = runner.query(conn, query, new MapListHandler(), params.toArray());
                for (Map<String, Object> m: rs) {
                    String tableName = (String) m.get("TABLE_NAME");
                    String fkConstraintName = (String) m.get("CONSTRAINT_NAME");

                    String rschema = (String) m.get("R_OWNER");
                    String uniqueConstraintName = (String) m.get("R_CONSTRAINT_NAME");

                    Table fkTable = schema.getTable(tableName);
                    UniqueConstraint uc = catalog.getSchema(rschema).getUniqueConstraint(uniqueConstraintName); // TODO check if exists

                    BiMap<UniqueConstraintColumn, Column> mapping = fkNameToColumnMapping.get(fkConstraintName);
                    ForeignKey foreignKey = fkTable.addForeignKey(fkConstraintName, uc, mapping);
                    uc.addForeignKey(foreignKey);
                    mapping.forEach((ucc, column) -> column.addForeignKey(foreignKey));
                }
            }
        }
    }
}
