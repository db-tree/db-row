package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
import me.vzhilin.dbrow.catalog.CatalogFilter;
import me.vzhilin.dbrow.catalog.filter.AcceptSchema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.SQLException;

public final class PostgresCatalogTest extends AbstractCatalogTest {
    @Override
    protected void cleanup(DataSource ds) throws SQLException {
        QueryRunner runner = new QueryRunner(ds);
        runner.update("DROP TABLE IF EXISTS \"B\"");
        runner.update("DROP TABLE IF EXISTS \"A\"");
    }

    @Override
    protected DataSource setupDatasource() throws SQLException {
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
    protected CatalogFilter getSchemaFilter(String schemaName) {
        return new AcceptSchema(schemaName);
    }

    @Override
    protected String numberColumnType() {
        return "numeric";
    }
}
