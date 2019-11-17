package me.vzhilin.dbrow.db.env;

import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.dbrow.db.adapter.OracleTestDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbcp2.BasicDataSource;

public final class OracleTestEnvironment extends CatalogTestEnvironment {
    public OracleTestEnvironment() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("oracle.jdbc.OracleDriver");
        ds.setUsername("system");
        ds.setPassword("test");
        ds.setUrl("jdbc:oracle:thin:@localhost:1521:XE");
        setDs(ds);

        setAdapter(new OracleDatabaseAdapter());
        setTestAdapter(new OracleTestDatabaseAdapter(ds));
        setNumberColumnType("number");
    }
}
