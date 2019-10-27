package me.vzhilin.dbrow.catalog.loader;

import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.CatalogFilter;

import javax.sql.DataSource;
import java.sql.SQLException;

public interface CatalogLoader {
    Catalog load(DataSource ds) throws SQLException;

    Catalog load(DataSource ds, CatalogFilter filter) throws SQLException;
}
