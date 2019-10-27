package me.vzhilin.dbrow.catalog.sql;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.ForeignKey;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.UniqueConstraint;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class OracleCatalogExporter {
    public void export(Catalog cat, PrintWriter out) {
        exportTables(cat, out);
        exportForeignKeys(cat, out);
    }

    private void exportForeignKeys(Catalog cat, PrintWriter out) {
        List<String> constraints = new ArrayList<>();

        cat.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                for (ForeignKey fk: table.getForeignKeys()) {
                    List<String> fkColumnNames = new ArrayList<>();
                    List<String> ucColumnNames = new ArrayList<>();
                    fk.getColumnMapping().forEach((ucc, fkc) -> {
                        fkColumnNames.add(fkc.getColumn().getName());
                        ucColumnNames.add(ucc.getColumn().getName());
                    });
                    String fkColumns = Joiner.on(',').join(fkColumnNames);
                    String ucColumns = Joiner.on(',').join(ucColumnNames);

                    UniqueConstraint uc = fk.getUniqueConstraint();
                    Table ucTable = uc.getTable();
                    constraints.add(String.format(
                        "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES \"%s\".\"%s\" (%s);",
                        table.getSchemaName(), table.getName(), fk.getFkName(), fkColumns,
                        ucTable.getSchemaName(), ucTable.getName(), ucColumns
                    ));
                }
            }
        });

        Collections.sort(constraints);
        out.print(Joiner.on('\n').join(constraints));
    }

    private void exportTables(Catalog cat, PrintWriter out) {
        List<String> tables = new ArrayList<>();
        List<String> constraints = new ArrayList<>();


        cat.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("CREATE TABLE \"%s\".\"%s\" (\n", table.getSchemaName(), table.getName()));
                List<String> parts = new ArrayList<>();
                table.getColumns().forEach((name, column) -> parts.add("\t" + name + " " + column.getDataType()));

                table.getUniqueConstraints().forEach(new Consumer<>() {
                    @Override
                    public void accept(UniqueConstraint uc) {
                        String columns = Joiner.on(',').join(uc.getColumnNames());
                        constraints.add(String.format("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT %s UNIQUE (%s);",
                            table.getSchemaName(), table.getName(),
                            uc.getName(), columns));
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
