package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.mariadb.MariadbDatabaseAdapter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.SQLException;

public final class MariaDBCatalogTest extends AbstractCatalogTest {
    @Override
    protected void cleanup(DataSource ds) throws SQLException {
        QueryRunner runner = new QueryRunner(ds);
        runner.update("DROP TABLE IF EXISTS B");
        runner.update("DROP TABLE IF EXISTS A");
    }

    @Override
    protected DataSource setupDatasource() throws SQLException {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUsername("test");
        ds.setPassword("test");
        ds.setUrl("jdbc:mariadb://localhost:3306/test");
        return ds;
    }

    @Override
    protected DatabaseAdapter newAdapter() {
        return new MariadbDatabaseAdapter();
    }

    @Override
    protected String numberColumnType() {
        return "DECIMAL";
    }
}
