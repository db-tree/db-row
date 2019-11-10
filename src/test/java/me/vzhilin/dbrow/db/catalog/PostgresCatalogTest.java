package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;

public final class PostgresCatalogTest extends AbstractCatalogTest {
    @Override
    protected DataSource setupDatasource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername("user");
        ds.setPassword("user");
        ds.setUrl("jdbc:postgresql://localhost:5432/test?autoReconnect=true");
        return ds;
    }

    @Override
    protected DatabaseAdapter newAdapter() {
        return new PostgresqlAdapter();
    }

    @Override
    protected String numberColumnType() {
        return "numeric";
    }
}
