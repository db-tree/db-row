package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.DatabaseAdapter;

public final class CatalogTestEnvironment {
    private String driverClassName;
    private String username;
    private String password;
    private String jdbcUrl;

    private String numberColumnType;
    private DatabaseAdapter adapter;

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
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

    @Override
    public String toString() {
        return driverClassName + ": " + jdbcUrl;
    }
}
