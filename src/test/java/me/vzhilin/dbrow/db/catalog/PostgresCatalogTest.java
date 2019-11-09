package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
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

public class PostgresCatalogTest {
    private static DataSource DS;

    @BeforeAll
    public static void prepareDatasource() {
        Locale.setDefault(Locale.US);

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername("user");
        ds.setPassword("user");
        ds.setUrl("jdbc:postgresql://localhost:5432/test?autoReconnect=true");

        DS = ds;
    }

    @BeforeAll
    public static void cleanBefore() throws SQLException {
        QueryRunner runner = new QueryRunner(DS);
        runner.update("DROP TABLE IF EXISTS \"B\"");
        runner.update("DROP TABLE IF EXISTS \"A\"");
    }

    @AfterAll
    public static void cleanAfter() throws SQLException {
        QueryRunner runner = new QueryRunner(DS);
        runner.update("DROP TABLE IF EXISTS \"B\"");
        runner.update("DROP TABLE IF EXISTS \"A\"");
    }

    @Test
    public void test() throws SQLException {
        Catalog sample = prepareCatalog();
        createTables(sample);

        assertEquals(sample, loadCatalog());
    }

    private void compare(Catalog sample, Catalog result) {
        String sqlSample = exportSql(sample);
        String sqlResult = exportSql(result);
        assertEquals(sqlSample, sqlResult);
    }

    private String exportSql(Catalog sample) {
        OracleCatalogExporter exporter = new OracleCatalogExporter();
        StringWriter sw = new StringWriter();
        exporter.export(new PostgresqlAdapter(), sample, new PrintWriter(sw));
        return sw.toString();
    }

    private Catalog loadCatalog() throws SQLException {
        return  new CatalogLoaderFactory().getLoader(DS).load(DS, new AcceptSchema("public"));
    }

    private void createTables(Catalog sample) throws SQLException {
        StringWriter sw = new StringWriter();
        new OracleCatalogExporter().export(new PostgresqlAdapter(), sample, new PrintWriter(sw));
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
        Schema schema = sample.addSchema("public");
        Table aTable = schema.addTable("A");
        aTable.addColumn("A", "numeric", 0);
        aTable.addColumn("B", "numeric", 1);
        aTable.addColumn("C", "numeric", 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint("uc_test_a", new String[]{"A"});
        UniqueConstraint ucBC = aTable.addUniqueConstraint("uc_test_bc", new String[]{"B", "C"});

        Table bTable = schema.addTable("B");
        bTable.addColumn("D", "numeric", 0);
        bTable.addColumn("E", "numeric", 1);
        bTable.addColumn("F", "numeric", 2);

        BiMap<UniqueConstraintColumn, Column> fkDMapping = new BiMap<>();
        fkDMapping.put(ucA.getColumn("A"), bTable.getColumn("D"));
        bTable.addForeignKey("fk_b_d", ucA, fkDMapping);

        BiMap<UniqueConstraintColumn, Column> fkEfMapping = new BiMap<>();
        fkEfMapping.put(ucBC.getColumn("B"), bTable.getColumn("E"));
        fkEfMapping.put(ucBC.getColumn("C"), bTable.getColumn("F"));
        bTable.addForeignKey("fk_b_ef", ucBC, fkEfMapping);
        return sample;
    }
}
