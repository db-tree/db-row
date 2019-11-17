package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.db.adapter.TestDatabaseAdapter;
import org.apache.commons.dbcp2.BasicDataSource;

public class CatalogTestEnvironment {
    private BasicDataSource ds;

    private String numberColumnType;
    private DatabaseAdapter adapter;
    private TestDatabaseAdapter testAdapter;

    public TestDatabaseAdapter getTestAdapter() {
        return testAdapter;
    }

    public void setTestAdapter(TestDatabaseAdapter testAdapter) {
        this.testAdapter = testAdapter;
    }

    public String getNumberColumnType() {
        return numberColumnType;
    }

    public void setNumberColumnType(String numberColumnType) {
        this.numberColumnType = numberColumnType;
    }

    public DatabaseAdapter getAdapter() {
        return adapter;
    }

    public void setAdapter(DatabaseAdapter adapter) {
        this.adapter = adapter;
    }

    protected void setDs(BasicDataSource ds) {
        this.ds = ds;
    }

    public BasicDataSource getDs() {
        return ds;
    }
}
