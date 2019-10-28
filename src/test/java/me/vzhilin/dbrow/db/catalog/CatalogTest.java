package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.catalog.sql.OracleCatalogExporter;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
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

public class CatalogTest {
    private static DataSource DS;

    @BeforeAll
    public static void prepareDatasource() throws SQLException {
        Locale.setDefault(Locale.US);

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("oracle.jdbc.OracleDriver");
        ds.setUsername("test");
        ds.setPassword("test");
        ds.setUrl("jdbc:oracle:thin:@localhost:1521:XE");

        DS = ds;

        cleanup();
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        QueryRunner runner = new QueryRunner(DS);
        for (Object m: runner.query("SELECT TABLE_NAME FROM USER_TABLES", new ArrayHandler())) {
            String tableName = (String) m;
            runner.update("DROP TABLE " + tableName + " CASCADE CONSTRAINTS");
        }
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
        exporter.export(sample, new PrintWriter(sw));
        return sw.toString();
    }

    private Catalog loadCatalog() throws SQLException {
        return  new CatalogLoaderFactory().getLoader(DS).load(DS, new AcceptSchema("TEST"));
    }

    private void createTables(Catalog sample) throws SQLException {
        StringWriter sw = new StringWriter();
        new OracleCatalogExporter().export(sample, new PrintWriter(sw));
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
        aTable.addColumn("A", "NUMBER", 0);
        aTable.addColumn("B", "NUMBER", 1);
        aTable.addColumn("C", "NUMBER", 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint("UC_TEST_A", new String[]{"A"});
        UniqueConstraint ucBC = aTable.addUniqueConstraint("UC_TEST_BC", new String[]{"B", "C"});

        Table bTable = schema.addTable("B");
        bTable.addColumn("D", "NUMBER", 0);
        bTable.addColumn("E", "NUMBER", 1);
        bTable.addColumn("F", "NUMBER", 2);

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
