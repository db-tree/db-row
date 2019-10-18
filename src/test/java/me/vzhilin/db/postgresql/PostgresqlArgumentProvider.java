package me.vzhilin.db.postgresql;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public final class PostgresqlArgumentProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
        List<Arguments> args = new ArrayList<>();
        args.add(Arguments.of("smallint", 1002, "1002"));
        args.add(Arguments.of("integer", 1003, "1003"));
        args.add(Arguments.of("bigint", 1004, "1004"));
        args.add(Arguments.of("decimal", 1005, "1005"));
        args.add(Arguments.of("numeric", 1006, "1006"));
        args.add(Arguments.of("real", 1006.5d, "1006.5"));
        args.add(Arguments.of("double precision", 1007.5d, "1007.5"));
        args.add(Arguments.of("smallserial", 1008, "1008"));
        args.add(Arguments.of("serial", 1009, "1009"));
        args.add(Arguments.of("bigserial", 1010, "1010"));
//        args.add(Arguments.of("money", 1011, "1011")); FIXME Money

        args.add(Arguments.of("text", "text_1", "text_1"));
        args.add(Arguments.of("character(10)", "text_2", "text_2"));
        args.add(Arguments.of("char(10)", "text_3", "text_3"));
        args.add(Arguments.of("character varying(10)", "text_4", "text_4"));
        args.add(Arguments.of("varchar(10)", "text_5", "text_5"));

        args.add(Arguments.of("boolean", true, "true"));
        args.add(Arguments.of("boolean", false, "false"));
        args.add(Arguments.of("uuid", UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
        return args.stream();
    }
}
