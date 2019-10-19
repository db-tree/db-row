package me.vzhilin.dbrow.db;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.dbrow.catalog.*;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RowTests {
    private QueryRunner runner;
    private BasicDataSource ds;

    @BeforeAll
    public void setUp() {
        ds = new BasicDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setUrl("jdbc:sqlite::memory:");
        runner = new QueryRunner(ds);
    }

    @AfterAll
    public void tearDown() throws SQLException {
        ds.close();
    }

    @Test
    public void rowTests() throws SQLException {
        runner.update("CREATE TABLE A ( " +
                "pk_a_1 INTEGER, " +
                "pk_a_2 TEXT, " +
                "text_a TEXT, " +
                "PRIMARY KEY (pk_a_1, pk_a_2))");

        runner.update("CREATE TABLE B ( " +
                "pk_b INTEGER, " +
                "fk_b_1 INTEGER, " +
                "fk_b_2 TEXT, " +
                "PRIMARY KEY (pk_b) " +
                "FOREIGN KEY (fk_b_1, fk_b_2) REFERENCES A(pk_a_1, pk_a_2))");

        runner.update("INSERT INTO A(pk_a_1, pk_a_2, text_a) VALUES (100, 200, '100_200')");
        runner.update("INSERT INTO A(pk_a_1, pk_a_2, text_a) VALUES (101, 201, '101_201')");
        runner.update("INSERT INTO A(pk_a_1, pk_a_2, text_a) VALUES (102, 202, '102_202')");

        runner.update("INSERT INTO B(pk_b, fk_b_1, fk_b_2) VALUES (300, 100, 200)");
        runner.update("INSERT INTO B(pk_b, fk_b_1, fk_b_2) VALUES (301, 100, 200)");
        runner.update("INSERT INTO B(pk_b, fk_b_1, fk_b_2) VALUES (302, 100, 200)");

        OracleDatabaseAdapter oracle = new OracleDatabaseAdapter();
        Catalog catalog = new CatalogLoader(oracle).load(ds, null);
        Schema schema = catalog.getOnlySchema();
        Table tableA = schema.getTable("A");
        Table tableB = schema.getTable("B");
        Column a1 = tableA.getColumn("pk_a_1");
        Column a2 = tableA.getColumn("pk_a_2");
        Column a3 = tableA.getColumn("text_a");

        Column b1 = tableB.getColumn("fk_b_1");
        Column b2 = tableB.getColumn("fk_b_2");

        Connection conn = ds.getConnection();
        RowContext ctx = new RowContext(catalog, oracle, conn, runner);
        ObjectKeyBuilder builder = new ObjectKeyBuilder(tableA);
        builder.set("pk_a_1", 100);
        builder.set("pk_a_2", 200);
        Row aRow = new Row(ctx, builder.build());
        assertThat(aRow.get(a3), equalTo("100_200"));

        ForeignKey foreignKey = Iterables.getOnlyElement(tableB.getForeignKeys().values());
        assertThat(aRow.backwardReferencesCount(), equalTo(Collections.singletonMap(foreignKey, 3)));

        List<Row> refs = Lists.newArrayList(aRow.backwardReference(foreignKey));
        assertThat(refs, hasSize(3));
        for (Row v: refs) {
            assertThat(v.get(b1), equalTo(100));
            assertThat(v.get(b2), equalTo("200"));
        }
    }
}
