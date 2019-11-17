package me.vzhilin.dbrow.db.env;

import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;

public final class PostgresTestEnvironment extends CatalogTestEnvironment {
    public PostgresTestEnvironment() {
        setDriverClassName("org.postgresql.Driver");
        setUsername("dbrow");
        setPassword("dbrow");
        setJdbcUrl("jdbc:postgresql://localhost:5432/dbrow");
        setAdapter(new PostgresqlAdapter());
        setNumberColumnType("numeric");
    }
}
