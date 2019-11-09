package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.mariadb.MariadbDatabaseAdapter;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.catalog.sql.OracleCatalogExporter;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MariaDBCatalogTest {
    private static DataSource DS;

    @BeforeAll
    public static void prepareDatasource() throws SQLException {
        Locale.setDefault(Locale.US);

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUsername("test");
        ds.setPassword("test");
        ds.setUrl("jdbc:mariadb://localhost:3306/test");

        DS = ds;

        cleanup();
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        QueryRunner runner = new QueryRunner(DS);
        runner.update("DROP TABLE IF EXISTS B");
        runner.update("DROP TABLE IF EXISTS A");
    }

    @Test
    public void test() throws SQLException {
        Catalog sample = prepareCatalog();
        createTables(sample);

        Catalog result = loadCatalog();
        assertEquals(sample, result);
    }

    private Catalog loadCatalog() throws SQLException {
        return new CatalogLoaderFactory().getLoader(DS).load(DS, new AcceptSchema("test"));
    }

    private void createTables(Catalog sample) throws SQLException {
        StringWriter sw = new StringWriter();
        new OracleCatalogExporter().export(new MariadbDatabaseAdapter(), sample, new PrintWriter(sw));
        String commands = sw.toString();
        executeCommands(commands);
    }

    private void executeCommands(String commands) throws SQLException {
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

    private Catalog prepareCatalog() {
        Catalog sample = new Catalog();
        Schema schema = sample.addSchema("test");
        Table aTable = schema.addTable("a");
        aTable.addColumn("a", "DECIMAL", 0);
        aTable.addColumn("b", "DECIMAL", 1);
        aTable.addColumn("c", "DECIMAL", 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint("uc_test_a", new String[]{"a"});
        UniqueConstraint ucBC = aTable.addUniqueConstraint("uc_test_bc", new String[]{"b", "c"});

        Table bTable = schema.addTable("b");
        bTable.addColumn("d", "DECIMAL", 0);
        bTable.addColumn("e", "DECIMAL", 1);
        bTable.addColumn("f", "DECIMAL", 2);

        BiMap<UniqueConstraintColumn, Column> fkDMapping = new BiMap<>();
        fkDMapping.put(ucA.getColumn("a"), bTable.getColumn("d"));
        bTable.addForeignKey("fk_b_d", ucA, fkDMapping);

        BiMap<UniqueConstraintColumn, Column> fkEfMapping = new BiMap<>();
        fkEfMapping.put(ucBC.getColumn("b"), bTable.getColumn("e"));
        fkEfMapping.put(ucBC.getColumn("c"), bTable.getColumn("f"));
        bTable.addForeignKey("fk_b_ef", ucBC, fkEfMapping);
        return sample;
    }
}
