package me.vzhilin.db.mariadb;

import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.mariadb.MariadbDatabaseAdapter;
import me.vzhilin.catalog.Catalog;
import me.vzhilin.catalog.CatalogLoader;
import me.vzhilin.catalog.filter.AcceptSchema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;

import java.sql.Connection;
import java.sql.SQLException;

public class MariadbDemo {
    public static void main(String... argv) throws SQLException {
        new MariadbDemo().start();
    }

    private void start() throws SQLException {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setUsername("test");
        ds.setPassword("test");
        ds.setUrl("jdbc:mariadb://localhost:3306/northwind");
        QueryRunner runner = new QueryRunner(ds);

        DatabaseAdapter mariaDb = new MariadbDatabaseAdapter();
        Connection connection = ds.getConnection();
        Catalog catalog = new CatalogLoader(mariaDb).load(ds, new AcceptSchema(mariaDb.defaultSchema(connection)));
        connection.close();

        System.err.println(catalog);
    }
}
