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
        compare(sample, result);
    }

    private void compare(Catalog sample, Catalog result) {
        String sqlSample = exportSql(sample);
        String sqlResult = exportSql(result);
        assertEquals(sqlSample, sqlResult);
    }

    private String exportSql(Catalog sample) {
        OracleCatalogExporter exporter = new OracleCatalogExporter();
        StringWriter sw = new StringWriter();
        exporter.export(new MariadbDatabaseAdapter(), sample, new PrintWriter(sw));
        return sw.toString();
    }

    private Catalog loadCatalog() throws SQLException {
        return  new CatalogLoaderFactory().getLoader(DS).load(DS, new AcceptSchema("test"));
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
        Schema schema = sample.addSchema("TEST");
        Table aTable = schema.addTable("A");
        aTable.addColumn("A", "DECIMAL", 0);
        aTable.addColumn("B", "DECIMAL", 1);
        aTable.addColumn("C", "DECIMAL", 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint("UC_TEST_A", new String[]{"A"});
        UniqueConstraint ucBC = aTable.addUniqueConstraint("UC_TEST_BC", new String[]{"B", "C"});

        Table bTable = schema.addTable("B");
        bTable.addColumn("D", "DECIMAL", 0);
        bTable.addColumn("E", "DECIMAL", 1);
        bTable.addColumn("F", "DECIMAL", 2);

        BiMap<UniqueConstraintColumn, Column> fkDMapping = new BiMap<>();
        fkDMapping.put(ucA.getColumn("A"), bTable.getColumn("D"));
        bTable.addForeignKey("FK_B_D", ucA, fkDMapping);

        BiMap<UniqueConstraintColumn, Column> fkEfMapping = new BiMap<>();
        fkEfMapping.put(ucBC.getColumn("B"), bTable.getColumn("E"));
        fkEfMapping.put(ucBC.getColumn("C"), bTable.getColumn("F"));
        bTable.addForeignKey("FK_B_EF", ucBC, fkEfMapping);
        return sample;
    }
}
