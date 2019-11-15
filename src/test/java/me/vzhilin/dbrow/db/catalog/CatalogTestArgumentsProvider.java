package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.db.env.MariadbTestEnvironment;
import me.vzhilin.dbrow.db.env.OracleTestEnvironment;
import me.vzhilin.dbrow.db.env.PostgresTestEnvironment;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CatalogTestArgumentsProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
        List<Arguments> args = new ArrayList<>();
        CatalogTestEnvironment oracle = new OracleTestEnvironment();
        oracle.setNumberColumnType("NUMBER");

        CatalogTestEnvironment mariaDb = new MariadbTestEnvironment();
        mariaDb.setNumberColumnType("decimal");

        CatalogTestEnvironment postgres = new PostgresTestEnvironment();
        postgres.setNumberColumnType("numeric");

        args.add(Arguments.of(oracle));
        args.add(Arguments.of(postgres));
        args.add(Arguments.of(mariaDb));
        return args.stream();
    }
}
