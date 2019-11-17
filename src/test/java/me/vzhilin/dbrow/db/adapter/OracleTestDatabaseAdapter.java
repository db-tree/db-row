package me.vzhilin.dbrow.db.adapter;

import org.apache.commons.dbcp2.BasicDataSource;

public final class OracleTestDatabaseAdapter extends TestDatabaseAdapter {
    public OracleTestDatabaseAdapter(BasicDataSource ds) {
        super(ds);
    }

    @Override
    protected void doCreateUser(String user, String password) {
        exec("create user " + user + " identified by " + password);
        exec("grant connect to " + user);
        exec("grant all privileges to " + user);
    }

    @Override
    protected void dropUser(String user) {
        exec("drop user " + user + " cascade");
    }

}
