package me.vzhilin.dbrow.catalog.loader;

import com.google.common.collect.HashMultimap;
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
    @Override
    protected void loadForeignConstraints(QueryRunner runner, Connection conn, Catalog catalog) throws SQLException {
        Map<String, Table> uniqConstraintToTableName = new HashMap<>();
        String uniqConstraintQuery =
            "select owner, table_name, constraint_name\n" +
            "from user_constraints uc \n" +
            "where constraint_type in ('U', 'P')";

        for (Map<String, Object> m: runner.query(conn, uniqConstraintQuery, new MapListHandler())) {
            String schemaName = (String) m.get("OWNER");
            String tableName = (String) m.get("TABLE_NAME");
            String constraintName = (String) m.get("CONSTRAINT_NAME");
            uniqConstraintToTableName.put(constraintName, catalog.getTable(schemaName, tableName));
        }

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
            "from user_constraints uc \n" +
            "join user_cons_columns fk on fk.constraint_name = uc.constraint_name\n" +
            "join user_cons_columns uc on uc.constraint_name = uc.r_constraint_name\n" +
            "where uc.constraint_type in ('R')\n" +
            "and fk.position = uc.position";

        for (Map<String, Object> m: runner.query(conn, columnQuery, new MapListHandler())) {
            String fkOwner = (String) m.get("OWNER");
            String fkTable = (String) m.get("TABLE_NAME");
            String fkConstraint = (String) m.get("FK_CONSTRAINT");

            int ucPos = ((Number) m.get("UC_POSITION")).intValue();
            String ucOwner = (String) m.get("R_OWNER");
            String ucConstraint = (String) m.get("UC_CONSTRAINT");

            int fkPos = ((Number) m.get("FK_POSITION")).intValue();
            String fkColumn = (String) m.get("FK_COLUMN");
            String ucColumn = (String) m.get("UC_COLUMN");

            Table fkT = catalog.getSchema(fkOwner).getTable(fkTable);
            Table ucT = uniqConstraintToTableName.get(ucConstraint);

            fkNameToColumnMapping
                .computeIfAbsent(fkConstraint, s -> new BiMap<>())
                .put(new UniqueConstraintColumn(ucT.getColumn(ucColumn), ucPos), fkT.getColumn(fkColumn));
        }

        String query =
            "select owner, table_name, constraint_name, r_owner, r_constraint_name\n" +
            "from user_constraints uc \n" +
            "where constraint_type in ('R')"; // TODO schema filter

        List<Map<String, Object>> rs = runner.query(conn, query, new MapListHandler());
        for (Map<String, Object> m: rs) {
            String schema = (String) m.get("OWNER");
            String tableName = (String) m.get("TABLE_NAME");
            String fkConstraintName = (String) m.get("CONSTRAINT_NAME");

            String rschema = (String) m.get("R_OWNER");
            String uniqueConstraintName = (String) m.get("R_CONSTRAINT_NAME");

            Schema s = catalog.getSchema(schema);
            Table fkTable = s.getTable(tableName);
            Table uniqTable = uniqConstraintToTableName.get(uniqueConstraintName);
            UniqueConstraint uc = uniqTable.getUniqueConstraint(uniqueConstraintName);

            fkTable.addForeignKey(fkConstraintName, uc, fkNameToColumnMapping.get(fkConstraintName));
        }
    }

    @Override
    protected void loadUniqueConstraints(QueryRunner runner, Connection conn, Catalog catalog, DatabaseMetaData metadata) throws SQLException {
        // TODO set schema
        List<Map<String, Object>> rs = runner.query(conn, "select\n" +
                "    uc.owner,\n" +
                "    uc.constraint_name, \n" +
                "    uc.constraint_type, \n" +
                "    uc.table_name, \n" +
                "    ucc.column_name, \n" +
                "    ucc.position \n" +
                "from user_constraints uc \n" +
                "join USER_CONS_COLUMNS ucc on uc.constraint_name = ucc.constraint_name\n" +
                "where uc.constraint_type in ('U', 'P')", new MapListHandler());


        Map<String, Table> uniqueConstraintTable = new HashMap<>();
        Multimap<String, UniqueConstraintColumn> uniqueConstraintColumns = HashMultimap.create();
        for (Map<String, Object> m: rs) {
            String schema = (String) m.get("OWNER");
            String constraintName = (String) m.get("CONSTRAINT_NAME");
            String constraintType = (String) m.get("CONSTRAINT_TYPE");
            String tableName = (String) m.get("TABLE_NAME");
            String columnName = (String) m.get("COLUMN_NAME");
            int position = ((Number) m.get("POSITION")).intValue();

            Schema s = catalog.getSchema(schema);
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
            table.addUniqueConstraint(consName, columns);
        });
    }

}
