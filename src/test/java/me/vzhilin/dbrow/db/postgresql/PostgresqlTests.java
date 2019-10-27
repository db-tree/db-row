package me.vzhilin.dbrow.db.postgresql;

import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.loader.CatalogLoaderFactory;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;
import me.vzhilin.dbrow.search.CountOccurences;
import me.vzhilin.dbrow.search.SearchInTable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PostgresqlTests {
    private static BasicDataSource DS;
    private static PostgresqlAdapter ADAPTER;
    private static QueryRunner RUNNER;

    @BeforeAll
    public static void setUp() {
        String host = "localhost";
        int port = 5432;
        String dbName = "test";

        DS = new BasicDataSource();
        DS.setDriverClassName("org.postgresql.Driver");
        DS.setUrl("jdbc:postgresql://" + host + ":" + port + "/"+ dbName + "?autoReconnect=true");
        DS.setUsername("user");
        DS.setPassword("user");
        ADAPTER = new PostgresqlAdapter();

        RUNNER = new QueryRunner(DS);
    }

    @AfterAll
    public static void tearDown() throws SQLException {
        DS.close();
    }

    @BeforeEach
    public void cleanUp() throws SQLException {
        try (Connection conn = DS.getConnection()) {
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = conn.getMetaData().getTables(null, "public", "test_%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            for (String table: tables) {
                RUNNER.update(conn, "DROP TABLE " + table);
            }
        }
    }


    @ParameterizedTest
    @ArgumentsSource(PostgresqlArgumentProvider.class)
    public void testParams(String type, Object value, String textValue) throws SQLException {
        RUNNER.update(format("CREATE TABLE test_01 (id serial PRIMARY KEY, v %s)", type));
        RUNNER.update("INSERT INTO test_01(v) values(?)", value);

        Catalog catalog = new CatalogLoaderFactory().getLoader(DS).load(DS);
        try (Connection connection = DS.getConnection()) {
            RowContext ctx = new RowContext(catalog, ADAPTER, connection, RUNNER);
            Map<Table, Long> count = new CountOccurences(ctx, textValue).count();
            assertFalse(count.isEmpty());

            Iterable<Row> s = new SearchInTable(ctx, count.entrySet().iterator().next().getKey(), textValue).search();
            Iterator<Row> it = s.iterator();
            assertTrue(it.hasNext());
//            assertEquals(value, v); TODO
        }
    }
}
