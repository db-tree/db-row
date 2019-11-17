package me.vzhilin.dbrow.db.adapter;

import org.apache.commons.dbcp2.BasicDataSource;

public class PostgresTestDatabaseAdapter extends TestDatabaseAdapter {
    public PostgresTestDatabaseAdapter(BasicDataSource ds) {
        super(ds);
    }

    @Override
    protected void doCreateUser(String user, String password) {
        exec("create user \"" + user + "\" WITH SUPERUSER LOGIN PASSWORD \'" + password + "\'");
        exec("create schema " + "\"" + user + "\"");
    }

    @Override
    protected void dropUser(String user) {
        exec("drop schema \"" + user + "\" CASCADE");
        exec("drop user \"" + user + "\"");
    }
}
