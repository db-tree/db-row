package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.catalog.sql.SQLCatalogExporter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Scanner;

public class BaseTest {
    protected DatabaseAdapter adapter;

    protected Catalog loadCatalog(DataSource ds, String... schemas) throws SQLException {
        return new CatalogLoaderFactory().getLoader(ds).load(ds, new AcceptSchema(schemas));
    }

    protected void setupEnv(CatalogTestEnvironment env) throws SQLException {
        Locale.setDefault(Locale.US);
        this.adapter = env.getAdapter();
    }

    protected void createTables(DataSource ds, Catalog sample) throws SQLException {
        StringWriter sw = new StringWriter();
        new SQLCatalogExporter().export(adapter, sample, new PrintWriter(sw));
        String commands = sw.toString();
        executeCommands(ds, commands);
    }

    protected void executeCommandsSafely(DataSource ds, String commands) {
        try {
            executeCommands(ds, commands);
        } catch (SQLException ex) {
            // do nothing
        }
    }

    protected void executeCommands(DataSource ds, String commands) throws SQLException {
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
}
