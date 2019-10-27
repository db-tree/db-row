package me.vzhilin.dbrow.catalog.sql;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.ForeignKey;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.UniqueConstraint;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class OracleCatalogExporter {
    public void export(Catalog cat, PrintStream out) {
        exportTables(cat, out);
        exportForeignKeys(cat, out);
    }

    private void exportForeignKeys(Catalog cat, PrintStream out) {
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
                    StringBuilder sb = new StringBuilder();
                    Table ucTable = uc.getTable();
                    sb.append(String.format(
                        "ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES \"%s\".\"%s\" (%s);",
                        table.getSchemaName(), table.getName(), fk.getFkName(), fkColumns,
                        ucTable.getSchemaName(), ucTable.getName(), ucColumns
                    ));
                    out.println(sb.toString());
                }
            }
        });
    }

    private void exportTables(Catalog cat, PrintStream out) {
        cat.forEachTable(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("CREATE TABLE \"%s\".\"%s\" (\n", table.getSchemaName(), table.getName()));

                List<String> parts = new ArrayList<>(table.getColumnCount());
                table.getColumns().forEach((name, column) -> parts.add("\t" + name + " " + column.getDataType()));

                table.getUniqueConstraints().forEach(new Consumer<>() {
                    @Override
                    public void accept(UniqueConstraint uc) {
                        String columns = Joiner.on(',').join(uc.getColumnNames());
                        parts.add(String.format("\tCONSTRAINT %s UNIQUE (%s)", uc.getName(), columns));
                    }
                });

                sb.append(Joiner.on(",\n").join(parts));
                sb.append("\n);");
                out.println(sb);
            }
        });
    }
}
