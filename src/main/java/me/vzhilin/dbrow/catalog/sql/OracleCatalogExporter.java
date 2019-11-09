package me.vzhilin.dbrow.catalog.sql;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.ForeignKey;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.UniqueConstraint;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class OracleCatalogExporter {
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
                    constraints.add(String.format(
                        "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);",
                        t1, fk.getFkName(), fkColumns,
                        t2, ucColumns
                    ));
                }
            }
        });

        Collections.sort(constraints);
        out.print(Joiner.on('\n').join(constraints));
    }

    private void exportTables(DatabaseAdapter adapter, Catalog cat, PrintWriter out) {
        List<String> tables = new ArrayList<>();
        List<String> constraints = new ArrayList<>();


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

                        constraints.add(String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);",
                            qualifiedTable, uc.getName(), columns));
                    }
                });

                sb.append(Joiner.on(",\n").join(parts));
                sb.append("\n);");
                tables.add(sb.toString());
            }
        });

        Collections.sort(tables);
        Collections.sort(constraints);

        StringBuilder sb = new StringBuilder();
        sb.append(Joiner.on('\n').join(tables));
        sb.append('\n');
        sb.append(Joiner.on('\n').join(constraints));

        out.println(sb);
    }
}
