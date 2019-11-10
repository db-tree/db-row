package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.IdentifierCase;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.catalog.sql.SQLCatalogExporter;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CatalogTest {
    private DataSource ds;
    private DatabaseAdapter adapter;
    private String currentSchema;

    private void cleanup() throws SQLException {
        try (Connection conn = ds.getConnection()) {
            adapter.dropTables(conn, Arrays.asList(s("B"), s("A")));
        }
    }

    private Catalog loadCatalog() throws SQLException {
        return new CatalogLoaderFactory().getLoader(ds).load(ds, new AcceptSchema(s(currentSchema)));
    }

    private void createTables(Catalog sample) throws SQLException {
        StringWriter sw = new StringWriter();
        new SQLCatalogExporter().export(adapter, sample, new PrintWriter(sw));
        String commands = sw.toString();
        executeCommands(commands);
    }

    protected void executeCommands(String commands) throws SQLException {
        Scanner sc = new Scanner(commands);
        sc.useDelimiter(";");

        QueryRunner runner = new QueryRunner(ds);
        while (sc.hasNext()) {
            String command = sc.next().trim();
            if (!command.isEmpty()) {
                runner.update(command);
            }
        }
    }

    private String s(String name) {
        IdentifierCase cs = adapter.getDefaultCase();
        switch (cs) {
            case LOWER:
                return name.toLowerCase();
            case UPPER:
                return name.toUpperCase();
            case NONE:
                return name;
            default:
                throw new RuntimeException();
        }
    }

    @ParameterizedTest
    @ArgumentsSource(CatalogTestArgumentsProvider.class)
    public void testCatalogLoader(CatalogTestEnvironment env) throws SQLException {
        Locale.setDefault(Locale.US);

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(env.getDriverClassName());
        ds.setUsername(env.getUsername());
        ds.setPassword(env.getPassword());
        ds.setUrl(env.getJdbcUrl());
        this.ds = ds;
        this.adapter = env.getAdapter();
        cleanup();

        try (Connection conn = ds.getConnection()){
            currentSchema = adapter.defaultSchema(conn);
        }

        Catalog sample = prepareCatalog(env.getNumberColumnType());
        createTables(sample);

        Catalog result = loadCatalog();
        assertEquals(sample, result);
        cleanup();
    }

    protected Catalog prepareCatalog(String columnType) {
        Catalog catalog = new Catalog();
        Schema schema = catalog.addSchema(s(currentSchema));
        Table aTable = schema.addTable(s("A"));
        String numberType = columnType;
        aTable.addColumn(s("A"), numberType, 0);
        aTable.addColumn(s("B"), numberType, 1);
        aTable.addColumn(s("C"), numberType, 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint(s("UC_TEST_A"), new String[]{s("A")});
        UniqueConstraint ucBC = aTable.addUniqueConstraint(s("UC_TEST_BC"), new String[]{s("B"), s("C")});

        Table bTable = schema.addTable(s("B"));
        bTable.addColumn(s("D"), numberType, 0);
        bTable.addColumn(s("E"), numberType, 1);
        bTable.addColumn(s("F"), numberType, 2);

        BiMap<UniqueConstraintColumn, Column> fkDMapping = new BiMap<>();
        fkDMapping.put(ucA.getColumn(s("A")), bTable.getColumn(s("D")));
        bTable.addForeignKey(s("FK_B_D"), ucA, fkDMapping);

        BiMap<UniqueConstraintColumn, Column> fkEfMapping = new BiMap<>();
        fkEfMapping.put(ucBC.getColumn(s("B")), bTable.getColumn(s("E")));
        fkEfMapping.put(ucBC.getColumn(s("C")), bTable.getColumn(s("F")));
        bTable.addForeignKey(s("FK_B_EF"), ucBC, fkEfMapping);
        return catalog;
    }
}
