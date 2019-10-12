package me.vzhilin.db;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.catalog.Catalog;
import me.vzhilin.catalog.CatalogLoader;
import me.vzhilin.catalog.Table;
import me.vzhilin.search.CountOccurences;
import me.vzhilin.search.SearchInTable;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class SearchTests {
    static {
        Locale.setDefault(Locale.US);
    }
    private QueryRunner runner;
    private BasicDataSource ds;

    @Before
    public void setUp() {
        ds = new BasicDataSource();
        ds.setDriverClassName("oracle.jdbc.OracleDriver");
        ds.setUrl("jdbc:oracle:thin:@localhost:1521:XE");
        ds.setUsername("test");
        ds.setPassword("test");
        runner = new QueryRunner(ds);
    }

    @After
    public void tearDown() throws SQLException {
        runner.update("DROP TABLE A");
        runner.update("DROP TABLE B");
        runner.update("DROP TABLE C");
        ds.close();
    }

    @Test
    public void search() throws SQLException {
        runner.update("CREATE TABLE A (" +
            "id int, " +
            "n1 NUMBER, " +
            "n2 VARCHAR2(20), " +
            "n3 DATE, " +
            "n4 BLOB, " +
            "CONSTRAINT XPK_A PRIMARY KEY (id)" +
        ")");

        runner.update("CREATE TABLE B (" +
            "id int, " +
            "n1 NUMBER, " +
            "n2 VARCHAR2(20), " +
            "n3 DATE, " +
            "n4 BLOB, " +
            "CONSTRAINT XPK_B PRIMARY KEY (id)" +
        ")");

        runner.update("CREATE TABLE C (" +
            "n1 NUMBER, " +
            "b BLOB " +
        ")");

        DatabaseAdapter oracle = new OracleDatabaseAdapter();

        runner.update("INSERT INTO A(id, n1) VALUES (1, 100)");
        runner.update("INSERT INTO A(id, n1) VALUES (2, 200)");
        runner.update("INSERT INTO A(id, n1) VALUES (3, 300)");

        runner.update("INSERT INTO B(id, n1) VALUES (1, 200)");
        runner.update("INSERT INTO B(id, n1) VALUES (2, 200)");
        runner.update("INSERT INTO B(id, n1) VALUES (3, 400)");

        Catalog catalog = new CatalogLoader(oracle).load(ds, "TEST");
        Connection conn = ds.getConnection();
        RowContext ctx = new RowContext(oracle, conn, runner);

        Table tableA = catalog.getSchema("TEST").getTable("A");
        Table tableB = catalog.getSchema("TEST").getTable("B");

        Map<Table, Long> occurences = new CountOccurences(catalog, runner, oracle).count("200");
        assertThat(occurences, hasEntry(tableA, 1L));
        assertThat(occurences, hasEntry(tableB, 2L));

        Iterable<Row> aResults = new SearchInTable(ctx, tableA).search("200");
        Iterator<Row> aIterator = aResults.iterator();
        Row a1 = aIterator.next();
        assertThat(a1.get("ID"), equalTo(new BigDecimal(2)));
        assertFalse(aIterator.hasNext());

        Iterable<Row> bResults = new SearchInTable(ctx, tableB).search("200");
        Iterator<Row> bIterator = bResults.iterator();
        Row b1 = bIterator.next();
        Row b2 = bIterator.next();
        assertFalse(bIterator.hasNext());
    }
}
