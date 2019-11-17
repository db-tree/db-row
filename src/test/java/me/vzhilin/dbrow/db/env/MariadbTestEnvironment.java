package me.vzhilin.dbrow.db.env;

import me.vzhilin.dbrow.adapter.mariadb.MariadbDatabaseAdapter;
import me.vzhilin.dbrow.db.adapter.MariadbTestDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbcp2.BasicDataSource;

public final class MariadbTestEnvironment extends CatalogTestEnvironment {
    public MariadbTestEnvironment() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUsername("root");
        ds.setPassword("dbrow-secret");
        ds.setUrl("jdbc:mariadb://localhost:3306/dbrow");

        setDs(ds);
        setAdapter(new MariadbDatabaseAdapter());
        setTestAdapter(new MariadbTestDatabaseAdapter(ds));
        setNumberColumnType("numeric");
    }
}
