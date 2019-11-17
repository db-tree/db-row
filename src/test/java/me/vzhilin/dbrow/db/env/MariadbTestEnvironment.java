package me.vzhilin.dbrow.db.env;

import me.vzhilin.dbrow.adapter.mariadb.MariadbDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;

public final class MariadbTestEnvironment extends CatalogTestEnvironment {
    public MariadbTestEnvironment() {
        setDriverClassName("org.mariadb.jdbc.Driver");
        setUsername("dbrow");
        setPassword("dbrow");
        setJdbcUrl("jdbc:mariadb://localhost:3306/dbrow");
        setAdapter(new MariadbDatabaseAdapter());
        setNumberColumnType("numeric");
    }
}
