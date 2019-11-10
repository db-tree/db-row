package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.IdentifierCase;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.catalog.sql.SQLCatalogExporter;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractCatalogTest {
    private DataSource DS;
    private DatabaseAdapter adapter;
    private String currentSchema;

    @BeforeEach
    public void setUp() throws SQLException {
        adapter = newAdapter();
        DS = setupDatasource();
        try (Connection conn = DS.getConnection()) {
            currentSchema = adapter.defaultSchema(conn);
        }
        cleanup();
    }

    @AfterEach
    public void tearDown() throws SQLException {
        cleanup();
    }

    private void cleanup() throws SQLException {
        try (Connection conn = DS.getConnection()) {
            adapter.dropTables(conn, Arrays.asList("B", "A"));
        }
    }

    protected abstract DataSource setupDatasource() throws SQLException;

    protected abstract DatabaseAdapter newAdapter();

    private Catalog loadCatalog() throws SQLException {
        return new CatalogLoaderFactory().getLoader(DS).load(DS, new AcceptSchema(s(currentSchema)));
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

        QueryRunner runner = new QueryRunner(DS);
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

    protected abstract String numberColumnType();

    @Test
    public void testCatalogLoader() throws SQLException {
        Catalog sample = prepareCatalog();
        createTables(sample);

        Catalog result = loadCatalog();
        assertEquals(sample, result);
    }

    protected Catalog prepareCatalog() {
        Catalog sample = new Catalog();
        Schema schema = sample.addSchema(s(currentSchema));
        Table aTable = schema.addTable(s("A"));
        String numberType = numberColumnType();
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
        return sample;
    }
}
