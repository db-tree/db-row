package me.vzhilin.dbrow.db.misc.types;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.Column;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.TableId;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import me.vzhilin.dbrow.db.env.MariadbTestEnvironment;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class MariadbCatalogLoaderTest extends BaseTest {
    @Override
    protected List<TableId> usedTables() {
        return Collections.singletonList(new TableId(currentSchema, s("x")));
    }

    @Test
    public void checkIfAbleLoadSupportedColumns() throws SQLException {
        CatalogTestEnvironment mariaDb = new MariadbTestEnvironment();
        setupEnv(mariaDb);

        cleanup();

        Map<String, CheckEntry> entries = new HashMap<>();
        int n = 0;
        for (ColumnTypeDescription ctd: adapter.getInfo().getColumnTypes().values()) {
            String name = ctd.getName();
            if ("enum".equals(name)) {
                continue;
            }
            if (!ctd.hasMandatoryLength()) {
                String cn = String.format("a%02d", n++);
                entries.put(cn, new CheckEntry(cn + " " + name, ctd.getAlias()));
            }

            if (ctd.hasLength()) {
                String cn = String.format("a%02d", n++);
                entries.put(cn, new CheckEntry(cn + " " + name + " (4)", ctd.getAlias(), 4));
            }

            if (ctd.hasPrecision()) {
                String cn = String.format("a%02d", n++);
                entries.put(cn, new CheckEntry(cn + " " + name + " (4, 2)", ctd.getAlias(), 4, 2));
            }
        }

        {
            String cn = String.format("a%02d", n++);
            entries.put(cn, new CheckEntry(cn + " enum('a', 'b')", "enum"));
        }

        String sql = "create table x(" +
                Joiner.on(',').join(entries.values().stream().map((Function<CheckEntry, String>) e -> e.name).collect(Collectors.toList())) + ")";
        executeCommands(sql);

        Catalog catalog = loadCatalog(s(currentSchema));
        Table xTable = catalog.getSchema(s(currentSchema)).getTable(s("X"));
        for (Column c: xTable.getColumns().values()) {
            CheckEntry e = entries.get(c.getName());

            assertEquals(e.getAlias(),  c.getDataType());
            if (e.getLength() != null) {
                assertEquals(e.getLength(), c.getLength());
            }

            if (e.getPrecision() != null) {
                assertEquals(e.getPrecision(), c.getPrecision());
            }
        }

        cleanup();
    }

    private final static class CheckEntry {
        private final String name;
        private final String alias;
        private final Integer length;
        private final Integer precision;

        private CheckEntry(String name, String alias) {
            this(name, alias, null);
        }

        private CheckEntry(String name, String alias, Integer length) {
            this(name, alias, length, null);
        }

        private CheckEntry(String name, String alias, Integer length, Integer precision) {
            this.name = name;
            this.alias = alias;
            this.length = length;
            this.precision = precision;
        }

        private String getName() {
            return name;
        }

        private String getAlias() {
            return alias;
        }

        private Integer getLength() {
            return length;
        }

        private Integer getPrecision() {
            return precision;
        }
    }
}
