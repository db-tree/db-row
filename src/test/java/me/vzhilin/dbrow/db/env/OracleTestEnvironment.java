package me.vzhilin.dbrow.db.env;

import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;

public final class OracleTestEnvironment extends CatalogTestEnvironment {
    public OracleTestEnvironment() {
        setDriverClassName("oracle.jdbc.OracleDriver");
        setUsername("C##DB_ROW");
        setPassword("DB_ROW");
        setJdbcUrl("jdbc:oracle:thin:@localhost:1521:XE");
        setAdapter(new OracleDatabaseAdapter());
        setNumberColumnType("number");
    }
}
