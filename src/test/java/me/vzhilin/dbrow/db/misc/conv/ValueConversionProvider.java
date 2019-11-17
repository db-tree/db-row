package me.vzhilin.dbrow.db.misc.conv;

import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import me.vzhilin.dbrow.db.env.MariadbTestEnvironment;
import me.vzhilin.dbrow.db.env.OracleTestEnvironment;
import me.vzhilin.dbrow.db.env.PostgresTestEnvironment;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class ValueConversionProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
        List<Arguments> args = new ArrayList<>();
        addOracle(args);
        addMariadb(args);
        addPostgres(args);
        return args.stream();
    }

    private void addPostgres(List<Arguments> args) throws SQLException {
        CatalogTestEnvironment postgres = new PostgresTestEnvironment();

        PGobject jsonObject = new PGobject();
        jsonObject.setType("json");
        jsonObject.setValue("{\"tags\":[{\"term\":\"paris\"}, {\"term\":\"food\"}]}");

        Object[][] expressions = new Object[][] {
            {"smallint", 1002, "1002"},
            {"integer", 1003, "1003"},
            {"bigint", 1004, "1004"},
            {"decimal", 1005, "1005"},
            {"numeric", 1006, "1006"},
            {"real", 1006.5, "1006.5"},
            {"double precision", 1007.5, "1007.5"},
            {"smallserial", 1008, "1008"},
            {"bigserial", 1009, "1009"},
            {"text", "text_1", "text_1"},
            {"character(6)", "text_2", "text_2"},
            {"char(6)", "text_3", "text_3"},
            {"character varying(10)", "text_4", "text_4"},
            {"varchar(10)", "text_5", "text_5"},
            {"boolean", true, "true"},
            {"boolean", false, "false"},
            {"integer[]", new int[]{1,2,3}, "[ARRAY]"},
            {"date", java.sql.Date.valueOf("1990-03-15"), "1990-03-15"},
            {"json", jsonObject, "{\"tags\":[{\"term\":\"paris\"}, {\"term\":\"food\"}]}"},
            {"uuid", UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}
        };
        args.add(Arguments.of(postgres, expressions));
    }

    private CatalogTestEnvironment addMariadb(List<Arguments> args) {
        CatalogTestEnvironment mariaDb = new MariadbTestEnvironment();
        Object[][] expressions = new Object[][]{
            {"tinyint", 4, "4"},
            {"boolean", 1, "true"},
            {"boolean", 0, "false"},
            {"mediumint", 5, "5"},
            {"int", 1000, "1000"},
            {"integer", 1001, "1001"},
            {"bigint", 1002, "1002"},
            {"decimal", 1003, "1003"},
            {"dec", 1003, "1003"},
            {"numeric", 1003, "1003"},
            {"fixed", 1003, "1003"},
            {"double", 2.15, "2.15"},
            {"double precision", 2.15, "2.15"},
            {"real", 2.15, "2.15"},
            {"char(3)", "abc", "abc"},
            {"varchar(3)", "abc", "abc"},
            {"binary(3)", "abc", "[BINARY]"},
            {"tinyblob", "abc", "[VARBINARY]"},
            {"mediumblob", "abc", "[VARBINARY]"},
            {"longblob", "abc", "[LONGVARBINARY]"},
            {"blob", "abc", "[VARBINARY]"},
            {"text", "abc", "abc"},
            {"mediumtext", "abc", "abc"},
            {"longtext", "abc", "abc"},
            {"tinytext", "abc", "abc"},
            {"json", "{\"k\":\"v\"}", "{\"k\":\"v\"}"},
            {"date", java.sql.Date.valueOf("1990-03-15"), "1990-03-15"},
            {"time", java.sql.Time.valueOf("12:34:56"), "12:34:56"},
            {"datetime", java.sql.Timestamp.valueOf("1990-03-15 12:34:56"), "1990-03-15 12:34:56.0"},
            {"year", "2020", "2020-01-01"},
//            {"char byte", "abc", "[char byte]"},
//            {"bit(8)", 0b111, "7"},
        };
        args.add(Arguments.of(mariaDb, expressions));
        return mariaDb;
    }

    private void addOracle(List<Arguments> args) {
        CatalogTestEnvironment oracle = new OracleTestEnvironment();
        Object[][] expressions = new Object[][] {
            {"varchar(4)", "text", "text"},
            {"char(4)", "text", "text"},
            {"nchar(4)", "text", "text"},
            {"NUMBER", "1000", "1000"},
            {"INTEGER", "1000", "1000"},
            {"INT", "1000", "1000"},
            {"SMALLINT", "1000", "1000"},
            {"FLOAT", "1000", "1000"},
            {"REAL", "1000", "1000"},
            {"DOUBLE PRECISION", "1000", "1000"},
            {"BINARY_FLOAT", "1000", "1000.0"},
            {"BINARY_DOUBLE", "1000", "1000.0"},
            {"DATE", java.sql.Date.valueOf("1990-03-15"), "1990-03-15 00:00:00.0"},
            {"TIMESTAMP", java.sql.Timestamp.valueOf("1990-03-15 04:05:06"), "1990-03-15 04:05:06.0"},
            {"TIMESTAMP WITH TIME ZONE", java.sql.Timestamp.valueOf("1990-03-15 04:05:06"), "1990-03-15 04:05:06.0"},
            {"TIMESTAMP WITH LOCAL TIME ZONE", java.sql.Timestamp.valueOf("1990-03-15 04:05:06"), "1990-03-15 04:05:06.0"},
            {"CLOB", "1000", "[CLOB]"},
            {"BLOB", "1000", "[BLOB]"},
//            {"BFILE", "1000", "[BFILE]"},
            {"RAW(1000)", "1000", "[VARBINARY]"},
            {"LONG RAW", "1000", "[LONGVARBINARY]"},
        };
        args.add(Arguments.of(oracle, expressions));
    }
}

