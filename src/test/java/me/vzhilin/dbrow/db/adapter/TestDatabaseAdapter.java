package me.vzhilin.dbrow.db.adapter;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class TestDatabaseAdapter {
    protected final BasicDataSource ds;
    private List<BasicDataSource> basicDataSources = new ArrayList<>();
    private List<String> users = new ArrayList<>();


    public TestDatabaseAdapter(BasicDataSource ds) {
        this.ds = ds;
    }

    public DataSource deriveDatasource(String user, String pass) {
        BasicDataSource d = new BasicDataSource();
        d.setDriverClassName(ds.getDriverClassName());
        d.setUrl(ds.getUrl());
        d.setUsername(user);
        d.setPassword(pass);
        basicDataSources.add(d);
        return d;
    }

    public final void createUser(String user, String password) {
        users.add(user);
        doCreateUser(user, password);
    }

    protected abstract void doCreateUser(String user, String password);
    protected abstract void dropUser(String user);

    protected void exec(String sql) {
        try {
            new QueryRunner(ds).update(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void teardown() {
        try {
            for (BasicDataSource basicDataSource : basicDataSources) {
                basicDataSource.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        for (String user: users) {
            dropUser(user);
        }
    }

    public DataSource getDataSource() {
        return ds;
    }
}
