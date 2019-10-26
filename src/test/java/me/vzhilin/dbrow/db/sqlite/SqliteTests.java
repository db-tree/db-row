package me.vzhilin.dbrow.db.sqlite;

import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.util.BiMap;
import org.junit.jupiter.api.Test;

public class SqliteTests {
    @Test
    void name() {
        Catalog catalog = new Catalog();
        Schema testSchema = catalog.addSchema("test_schema");
        Table tableA = testSchema.addTable("A");
        tableA.addColumn("aa", "text", 0);
        tableA.addColumn("ab", "text", 1);
        tableA.addColumn("ac", "text", 2);

        UniqueConstraint uniqueABC = tableA.addUniqueConstraint(new String[]{"aa", "ab", "ac"});
        UniqueConstraint uniqueAB = tableA.addUniqueConstraint(new String[]{"aa", "ab"});

        Table tableB = testSchema.addTable("B");
        tableB.addColumn("ba", "text", 0);
        tableB.addColumn("bb", "text", 1);
        tableB.addColumn("bc", "text", 2);
        tableB.addColumn("bd", "text", 3);
        tableB.addColumn("be", "text", 4);

        tableB.addUniqueConstraint(new String[]{"ba"});

        BiMap<UniqueConstraintColumn, Column> fk1 = new BiMap<>();
        fk1.put(uniqueAB.getColumn("aa"), tableB.getColumn("ba"));
        fk1.put(uniqueAB.getColumn("ab"), tableB.getColumn("bb"));

        BiMap<UniqueConstraintColumn, Column> fk2 = new BiMap<>();
        fk2.put(uniqueAB.getColumn("aa"), tableB.getColumn("bc"));
        fk2.put(uniqueAB.getColumn("ab"), tableB.getColumn("bd"));
        fk2.put(uniqueAB.getColumn("ac"), tableB.getColumn("be"));

        tableB.addForeignKey(uniqueAB, fk1);
        tableB.addForeignKey(uniqueAB, fk2);
    }
}
