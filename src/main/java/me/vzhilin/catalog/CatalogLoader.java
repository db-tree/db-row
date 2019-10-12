package me.vzhilin.catalog;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.util.BiMap;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class CatalogLoader {
    private final DatabaseAdapter adapter;

    public CatalogLoader(DatabaseAdapter adapter) {
        this.adapter = adapter;
    }

    public Catalog load(DataSource ds, String schemaName) throws SQLException {
        Catalog catalog = new Catalog();
        Connection conn = ds.getConnection();
        DatabaseMetaData metadata = conn.getMetaData();
        loadTables(catalog, schemaName, metadata);
        conn.close();
        return catalog;
    }

    private void loadTables(Catalog catalog, String schemaName, DatabaseMetaData metadata) throws SQLException {
        ResultSet tables = metadata.getTables(null, schemaName, null, new String[]{"TABLE"});
        while (tables.next()) {
            Schema schema = catalog.getSchema(tables.getString("TABLE_SCHEM"));
            schema.addTable(tables.getString("TABLE_NAME"));
        }
        tables.close();

        ResultSet columns = metadata.getColumns(null, schemaName, null, null);
        while (columns.next()) {
            Schema schema = catalog.getSchema(columns.getString("TABLE_SCHEM"));
            String tableName = columns.getString("TABLE_NAME");
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");
            int columnIndex = columns.getInt("ORDINAL_POSITION") - 1;
            Table table = schema.getTable(tableName);
            if (table != null) {
                table.addColumn(columnName, columnType, columnIndex, adapter.getType(columnType));
            }
        }
        columns.close();

        catalog.forEachTable(table -> {
            try {
                ResultSet primaryKeys = metadata.getPrimaryKeys(null, table.getSchemaName(), table.getName());
                PrimaryKey pk = null;
                while (primaryKeys.next()) {
                    String columnName = primaryKeys.getString("COLUMN_NAME");
                    int keySeq = primaryKeys.getInt("KEY_SEQ") - 1;
                    if (pk == null) {
                        String pkName = primaryKeys.getString("PK_NAME");
                        pk = new PrimaryKey(Optional.ofNullable(pkName), table);
                    }
                    Column pkColumn = table.getColumn(columnName);
                    pk.addColumn(pkColumn, keySeq);
                    pkColumn.setPrimaryKey(pk);
                }
                table.setPk(pk);
                primaryKeys.close();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });

        catalog.forEachTable(table -> {
            try {
                Optional<PrimaryKey> maybePk = table.getPrimaryKey();
                if (maybePk.isPresent()) {
                    PrimaryKey pk = maybePk.get();
                    ResultSet keys = metadata.getExportedKeys(null, schemaName, table.getName());

                    // fkTable, fkName, columnMapping
                    Map<Table, Map<String, BiMap<PrimaryKeyColumn, ForeignKeyColumn>>> columnMapping = new HashMap<>();

                    while (keys.next()) {
                        String pkColumnName = keys.getString("PKCOLUMN_NAME");
                        String fkTableName = keys.getString("FKTABLE_NAME");
                        String fkColumnName = keys.getString("FKCOLUMN_NAME");
                        String fkName = keys.getString("FK_NAME");
                        String schema = keys.getString("FKTABLE_SCHEM");
                        int fkPos = keys.getInt("KEY_SEQ");

                        Table fkTable = catalog.getSchema(schema).getTable(fkTableName);

                        PrimaryKeyColumn pkColumn = pk.getColumn(pkColumnName);
                        ForeignKeyColumn fkColumn = new ForeignKeyColumn(null, fkTable.getColumn(fkColumnName), fkPos);

                        columnMapping.
                                computeIfAbsent(fkTable, t -> new HashMap<>()).
                                computeIfAbsent(fkName, t -> new BiMap<>()).
                                put(pkColumn, fkColumn);
                    }
                    keys.close();

                    columnMapping.forEach((fkTable, fkNameToColumns) ->
                            fkNameToColumns.forEach((fkName, cols) -> {
                                ForeignKey foreignKey = fkTable.addForeignKey(fkName, table, cols);
                                pk.addForeignKey(foreignKey);
                                cols.forEach((pkc, fkc) -> fkc.getColumn().addForeignKey(foreignKey));
                            }));
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        });
    }
}
