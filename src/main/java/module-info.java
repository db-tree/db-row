module dbrow {
    exports me.vzhilin.dbrow.adapter;
    exports me.vzhilin.dbrow.adapter.conv;
    exports me.vzhilin.dbrow.adapter.mariadb;
    exports me.vzhilin.dbrow.adapter.oracle;
    exports me.vzhilin.dbrow.adapter.postgres;

    exports me.vzhilin.dbrow.catalog;
    exports me.vzhilin.dbrow.catalog.filter;
    exports me.vzhilin.dbrow.db;
    exports me.vzhilin.dbrow.search;
    exports me.vzhilin.dbrow.util;

    requires java.sql;
    requires java.management;
    requires com.google.common;
    requires commons.dbutils;
}