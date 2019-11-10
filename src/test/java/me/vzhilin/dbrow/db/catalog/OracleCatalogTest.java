package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.util.Locale;

public final class OracleCatalogTest extends AbstractCatalogTest {
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
