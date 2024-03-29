package me.vzhilin.dbrow.catalog.sql;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.mariadb.MariadbDatabaseAdapter;
import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
import me.vzhilin.dbrow.catalog.*;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SQLCatalogExporter {
    public void export(DatabaseAdapter adapter, Catalog cat, PrintWriter out) {
        exportTables(adapter, cat, out);
        exportForeignKeys(adapter, cat, out);
    }

    private void exportForeignKeys(DatabaseAdapter adapter, Catalog cat, PrintWriter out) {
        List<String> constraints = new ArrayList<>();
        cat.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                for (ForeignKey fk: table.getForeignKeys()) {
                    List<String> fkColumnNames = new ArrayList<>();
                    List<String> ucColumnNames = new ArrayList<>();
                    fk.getColumnMapping().forEach((ucc, fkc) -> {
                        fkColumnNames.add(adapter.qualifiedColumnName(fkc.getColumn().getName()));
                        ucColumnNames.add(adapter.qualifiedColumnName(ucc.getColumn().getName()));
                    });
                    String fkColumns = Joiner.on(',').join(fkColumnNames);
                    String ucColumns = Joiner.on(',').join(ucColumnNames);

                    UniqueConstraint uc = fk.getUniqueConstraint();
                    Table ucTable = uc.getTable();
                    String t1 = adapter.qualifiedTableName(table);
                    String t2 = adapter.qualifiedTableName(ucTable);
                    String fkName = adapter.qualifiedColumnName(fk.getFkName());

                    if (adapter instanceof OracleDatabaseAdapter) {
                        if (!Objects.equals(table.getSchemaName(), ucTable.getSchemaName())) {
                            constraints.add(
                                String.format("GRANT REFERENCES ON %s TO %s;",
                                    t2, adapter.qualifiedSchemaName(table.getSchemaName()))
                            );
                        }
                    }

                    constraints.add(String.format(
                        "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);",
                        t1, fkName, fkColumns,
                        t2, ucColumns
                    ));
                }
            }
        });

        out.print(Joiner.on('\n').join(constraints));
    }

    private void exportTables(DatabaseAdapter adapter, Catalog cat, PrintWriter out) {
        List<String> tables = new ArrayList<>();
        List<String> constraints = new ArrayList<>();

        if (adapter instanceof PostgresqlAdapter) {
            cat.forEachSchema(new Consumer<Schema>() {
                @Override
                public void accept(Schema schema) {
                    out.println("CREATE SCHEMA IF NOT EXISTS " + adapter.qualifiedSchemaName(schema.getName()) + ";");
                }
            });
        }

        if (adapter instanceof MariadbDatabaseAdapter) {
            cat.forEachSchema(new Consumer<Schema>() {
                @Override
                public void accept(Schema schema) {
                    out.println("CREATE DATABASE IF NOT EXISTS " + adapter.qualifiedSchemaName(schema.getName()) + ";");
                }
            });
        }

        cat.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                StringBuilder sb = new StringBuilder();
                String qualifiedTable = adapter.qualifiedTableName(table);
                sb.append(String.format("CREATE TABLE %s (\n", qualifiedTable));
                List<String> parts = new ArrayList<>();
                table.getColumns().forEach((name, column) -> parts.add("\t" + adapter.qualifiedColumnName(name) + " " + column.getDataType()));

                table.getUniqueConstraints().forEach(new Consumer<>() {
                    @Override
                    public void accept(UniqueConstraint uc) {
                        List<String> qualifiedColumns = uc.getColumnNames().stream().map(adapter::qualifiedColumnName).collect(Collectors.toList());
                        String columns = Joiner.on(',').join(qualifiedColumns);

                        String columnName = adapter.qualifiedColumnName(uc.getName());
                        constraints.add(String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
                            qualifiedTable, columnName, columns));
                    }
                });

                sb.append(Joiner.on(",\n").join(parts));
                sb.append("\n);");
                tables.add(sb.toString());
            }
        });


        StringBuilder sb = new StringBuilder();
        sb.append(Joiner.on('\n').join(tables));
        sb.append('\n');
        sb.append(Joiner.on('\n').join(constraints));

        out.println(sb);
    }
}
