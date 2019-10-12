package me.vzhilin.db;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.catalog.*;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;

public final class SchemaTests {
    private QueryRunner runner;
    private BasicDataSource ds;

    @Before
    public void setUp() {
        ds = new BasicDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setUrl("jdbc:sqlite::memory:");
        runner = new QueryRunner(ds);
    }

    @After
    public void tearDown() throws SQLException {
        ds.close();
    }

    @Test
    public void foreignKeys() throws SQLException {
        runner.update("CREATE TABLE A ( " +
                "pk_a_1 INTEGER, " +
                "pk_a_2 TEXT, " +
                "text_a TEXT, " +
                "PRIMARY KEY (pk_a_1, pk_a_2))");

        runner.update("CREATE TABLE B ( " +
                "fk_b_1 INTEGER, " +
                "fk_b_2 TEXT, " +
                "FOREIGN KEY (fk_b_1, fk_b_2) REFERENCES A(pk_a_1, pk_a_2))");

        DatabaseAdapter oracle = new OracleDatabaseAdapter();
        Catalog catalog = new CatalogLoader(oracle).load(ds, null);
        Schema schema = catalog.getOnlySchema();
        Table tableA = schema.getTable("A");
        Table tableB = schema.getTable("B");
        Optional<PrimaryKey> maybeAPk = tableA.getPrimaryKey();

        assertTrue(maybeAPk.isPresent());
        PrimaryKey apk = maybeAPk.get();
        PrimaryKeyColumn columnPkA1 = tableA.getPrimaryKeyColumn("pk_a_1");
        PrimaryKeyColumn columnPkA2 = tableA.getPrimaryKeyColumn("pk_a_2");
        Column columnTextA = tableA.getColumn("text_a");

        Column columnB1 = tableB.getColumn("fk_b_1");
        Column columnB2 = tableB.getColumn("fk_b_2");
        assertThat(columnPkA1.getColumn().getDataType(), equalTo("INTEGER"));
        assertThat(columnPkA2.getColumn().getDataType(), equalTo("TEXT"));
        assertThat(columnTextA.getDataType(), equalTo("TEXT"));
//        assertThat(apk.getColumns(), equalTo(Sets.newHashSet(columnPkA1, columnPkA2))); FIXME

//        BiMap<PrimaryKeyColumn, ForeignKeyColumn> columnMapping = new BiMap<>();
//        columnMapping.put(columnPkA1, columnB1);
//        columnMapping.put(columnPkA2, columnB2);
//
//        ForeignKey bForeignKey = Iterables.getOnlyElement(apk.getForeignKeys());
//        assertThat(bForeignKey.getColumnMapping(), equalTo(columnMapping));
        assertFalse(tableB.getPrimaryKey().isPresent());
    }

    @Test
    public void multipleForeignKeys() throws SQLException {
        runner.update("CREATE TABLE A (pk_a INTEGER, PRIMARY KEY(pk_a))");
        runner.update("CREATE TABLE B ( " +
                "fk_b_1 INTEGER, " +
                "fk_b_2 INTEGER, " +
                "fk_b_3 INTEGER, " +
                "CONSTRAINT fk1 FOREIGN KEY (fk_b_1) REFERENCES A(pk_a) " +
                "CONSTRAINT fk2 FOREIGN KEY (fk_b_2) REFERENCES A(pk_a) " +
                "CONSTRAINT fk3 FOREIGN KEY (fk_b_3) REFERENCES A(pk_a)) ");

        DatabaseAdapter oracle = new OracleDatabaseAdapter();
        Catalog catalog = new CatalogLoader(oracle).load(ds, null);
        Schema schema = catalog.getOnlySchema();
        Table tableA = schema.getTable("A");
        Table tableB = schema.getTable("B");

        PrimaryKey apk = tableA.getPrimaryKey().get();
        Set<ForeignKey> fks = apk.getForeignKeys();
        assertThat(fks, hasSize(3));
    }
}
