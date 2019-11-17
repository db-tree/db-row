package me.vzhilin.dbrow.db.adapter;

import org.apache.commons.dbcp2.BasicDataSource;

public class MariadbTestDatabaseAdapter extends TestDatabaseAdapter {
    public MariadbTestDatabaseAdapter(BasicDataSource ds) {
        super(ds);
    }

    @Override
    protected void doCreateUser(String user, String password) {
        exec("create database `" + user + "`");
        exec("create user '" + user + "'@'%' IDENTIFIED BY '" + password + "'");
        exec("GRANT ALL ON *.* TO " + "'" + user + "'" + "@'%' IDENTIFIED BY '" + password + "'");
        exec("FLUSH PRIVILEGES");
    }

    @Override
    protected void dropUser(String user) {
        exec("drop user `" + user + "`");
        exec("drop database `" + user + "`");
    }
}
