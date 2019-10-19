package me.vzhilin.dbrow.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.catalog.filter.AcceptAny;
import me.vzhilin.dbrow.util.BiMap;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

public class CatalogLoader {
    private final DatabaseAdapter adapter;

    public CatalogLoader(DatabaseAdapter adapter) {
        this.adapter = adapter;
    }

    public Catalog load(DataSource ds) throws SQLException {
        return load(ds, new AcceptAny());
    }

    public Catalog load(DataSource ds, CatalogFilter filter) throws SQLException {
        Catalog catalog = new Catalog();
        Connection conn = ds.getConnection();
        DatabaseMetaData metadata = conn.getMetaData();
        loadTables(catalog, filter, metadata);
        conn.close();
        return catalog;
    }

    private void loadTables(Catalog catalog, CatalogFilter filter, DatabaseMetaData metadata) throws SQLException {
        if (isMariaDb(metadata.getDriverName())) {
            try (ResultSet catalogs = metadata.getCatalogs()) {
                while (catalogs.next()) {
                    String schemaName = catalogs.getString("TABLE_CAT");
                    if (filter.acceptSchema(schemaName)) {
                        catalog.addSchema(schemaName);
                    }
                }
            }
        } else {
            try (ResultSet schemas = metadata.getSchemas()) {
                while (schemas.next()) {
                    String schemaName = schemas.getString("TABLE_SCHEM");
                    if (filter.acceptSchema(schemaName)) {
                        catalog.addSchema(schemaName);
                    }
                }
            }
        }

        catalog.forEachSchema(new Consumer<Schema>() {
              @Override
              public void accept(Schema schema) {
                  try {
                      ResultSet tables = metadata.getTables(null, schema.getName(), null, null);
                      while (tables.next()) {
                          String tableName = tables.getString("TABLE_NAME");
                          String tableType = tables.getString("TABLE_TYPE");

                          if ("TABLE".equals(tableType) && filter.acceptTable(schema.getName(), tableName)) {
                              schema.addTable(tableName);
                          }
                      }
                      tables.close();
                  } catch (SQLException ex) {
                      throw new RuntimeException(ex);
                  }
              }
          }
        );

        catalog.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                try {
                    ResultSet columns = metadata.getColumns(null, table.getSchemaName(), table.getName(), null);
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String columnType = columns.getString("TYPE_NAME");
                        int columnIndex = columns.getInt("ORDINAL_POSITION") - 1;
                        table.addColumn(columnName, columnType, columnIndex);
                    }
                    columns.close();
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                }
            }
        });

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
                    ResultSet keys = metadata.getExportedKeys(null, table.getSchemaName(), table.getName());

                    // fkTable, fkName, columnMapping
                    Map<Table, Map<String, BiMap<PrimaryKeyColumn, ForeignKeyColumn>>> columnMapping = new LinkedHashMap<>();

                    while (keys.next()) {
                        String pkColumnName = keys.getString("PKCOLUMN_NAME");
                        String fkTableName = keys.getString("FKTABLE_NAME");
                        String fkColumnName = keys.getString("FKCOLUMN_NAME");
                        String fkName = keys.getString("FK_NAME");
                        String schema = keys.getString("FKTABLE_SCHEM");
                        if (schema == null) {
                            schema = table.getSchemaName();
                        }
                        int fkPos = keys.getInt("KEY_SEQ");

                        Table fkTable = catalog.getSchema(schema).getTable(fkTableName);

                        PrimaryKeyColumn pkColumn = pk.getColumn(pkColumnName);
                        ForeignKeyColumn fkColumn = new ForeignKeyColumn(null, fkTable.getColumn(fkColumnName), fkPos);

                        columnMapping.
                            computeIfAbsent(fkTable, t -> new LinkedHashMap<>()).
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

    private boolean isMariaDb(String driverName) {
        return driverName.contains("MariaDB");
    }
}
