package me.vzhilin.dbrow.db.env;

import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
import me.vzhilin.dbrow.db.adapter.PostgresTestDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbcp2.BasicDataSource;

public final class PostgresTestEnvironment extends CatalogTestEnvironment {
    public PostgresTestEnvironment() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername("dbrow");
        ds.setPassword("dbrow");
        ds.setUrl("jdbc:postgresql://localhost:5432/dbrow");
        setTestAdapter(new PostgresTestDatabaseAdapter(ds));
        setAdapter(new PostgresqlAdapter());
        setNumberColumnType("numeric");
    }
}
