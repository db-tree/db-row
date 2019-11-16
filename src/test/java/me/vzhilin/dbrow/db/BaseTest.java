package me.vzhilin.dbrow.db;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.IdentifierCase;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.TableId;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.catalog.sql.SQLCatalogExporter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

public class BaseTest {
    protected DataSource ds;
    protected DatabaseAdapter adapter;
    protected String currentSchema;

    protected void cleanup() throws SQLException {
        try (Connection conn = ds.getConnection()) {
            adapter.dropTables(conn, usedTables());
        }
    }

    protected List<TableId> usedTables() {
        return Collections.emptyList();
    }

    protected Catalog loadCatalog(String... schemas) throws SQLException {
        return new CatalogLoaderFactory().getLoader(ds).load(ds, new AcceptSchema(schemas));
    }

    protected void setupSchema() throws SQLException {
        try (Connection conn = ds.getConnection()){
            currentSchema = adapter.defaultSchema(conn);
        }
    }

    protected void setupEnv(CatalogTestEnvironment env) throws SQLException {
        Locale.setDefault(Locale.US);
        this.ds = setupDataSource(env);
        this.adapter = env.getAdapter();
        cleanup();
        setupSchema();
    }

    protected BasicDataSource setupDataSource(CatalogTestEnvironment env) {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(env.getDriverClassName());
        ds.setUsername(env.getUsername());
        ds.setPassword(env.getPassword());
        ds.setUrl(env.getJdbcUrl());
        return ds;
    }

    protected void createTables(Catalog sample) throws SQLException {
        StringWriter sw = new StringWriter();
        new SQLCatalogExporter().export(adapter, sample, new PrintWriter(sw));
        String commands = sw.toString();
        executeCommands(commands);
    }

    protected void executeCommandsSafely(String commands) {
        try {
            executeCommands(commands);
        } catch (SQLException ex) {
            // do nothing
        }
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

    protected String s(String name) {
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
}
