package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.adapter.mariadb.MariadbDatabaseAdapter;
import me.vzhilin.dbrow.adapter.oracle.OracleDatabaseAdapter;
import me.vzhilin.dbrow.adapter.postgres.PostgresqlAdapter;
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
        CatalogTestEnvironment oracle = new CatalogTestEnvironment();
        oracle.setDriverClassName("oracle.jdbc.OracleDriver");
        oracle.setUsername("test");
        oracle.setPassword("test");
        oracle.setJdbcUrl("jdbc:oracle:thin:@localhost:1521:XE");
        oracle.setAdapter(new OracleDatabaseAdapter());
        oracle.setNumberColumnType("NUMBER");

        CatalogTestEnvironment mariaDb = new CatalogTestEnvironment();
        mariaDb.setDriverClassName("org.mariadb.jdbc.Driver");
        mariaDb.setUsername("test");
        mariaDb.setPassword("test");
        mariaDb.setJdbcUrl("jdbc:mariadb://localhost:3306/test");
        mariaDb.setAdapter(new MariadbDatabaseAdapter());
        mariaDb.setNumberColumnType("DECIMAL");

        CatalogTestEnvironment postgres = new CatalogTestEnvironment();
        postgres.setDriverClassName("org.postgresql.Driver");
        postgres.setUsername("user");
        postgres.setPassword("user");
        postgres.setJdbcUrl("jdbc:postgresql://localhost:5432/test?autoReconnect=true");
        postgres.setAdapter(new PostgresqlAdapter());
        postgres.setNumberColumnType("numeric");

        args.add(Arguments.of(oracle));
        args.add(Arguments.of(mariaDb));
        args.add(Arguments.of(postgres));
        return args.stream();
    }
}
