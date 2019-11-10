package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Locale;

public final class OracleCatalogTest extends AbstractCatalogTest {

    @Override
    protected void cleanup(DataSource ds) throws SQLException {
        QueryRunner runner = new QueryRunner(ds);
        for (Object m: runner.query("SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME IN (?, ?)", new ArrayHandler(),
                "A", "B")) {

            String tableName = (String) m;
            runner.update("DROP TABLE " + tableName + " CASCADE CONSTRAINTS");
        }
    }

    @Override
    protected DataSource setupDatasource() {
        Locale.setDefault(Locale.US);
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("oracle.jdbc.OracleDriver");
        ds.setUsername("test");
        ds.setPassword("test");
        ds.setUrl("jdbc:oracle:thin:@localhost:1521:XE");
        return ds;
    }

    @Override
    protected DatabaseAdapter newAdapter() {
        return new OracleDatabaseAdapter();
    }

    @Override
    protected String numberColumnType() {
        return "NUMBER";
    }
}
