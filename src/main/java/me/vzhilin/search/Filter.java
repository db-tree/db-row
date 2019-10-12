package me.vzhilin.search;

import me.vzhilin.catalog.Column;
import me.vzhilin.catalog.Schema;
import me.vzhilin.catalog.Table;

public interface Filter {
    boolean pass(Schema schema);
    boolean pass(Table table);
    boolean pass(Column column);
}
