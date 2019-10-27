package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.catalog.sql.OracleCatalogExporter;
import me.vzhilin.dbrow.util.BiMap;
import org.junit.jupiter.api.Test;

public class CatalogTest {
    @Test
    public void test() {
        Catalog cat = new Catalog();
        Schema schema = cat.addSchema("TEST");
        Table aTable = schema.addTable("A");
        aTable.addColumn("A", "int", 0);
        aTable.addColumn("B", "int", 1);
        aTable.addColumn("C", "int", 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint("UC_TEST_A", new String[]{"A"});
        UniqueConstraint ucBC = aTable.addUniqueConstraint("UC_TEST_BC", new String[]{"B", "C"});

        Table bTable = schema.addTable("B");
        bTable.addColumn("D", "int", 0);
        bTable.addColumn("E", "int", 1);
        bTable.addColumn("F", "int", 2);

        BiMap<UniqueConstraintColumn, Column> fkDMapping = new BiMap<>();
        fkDMapping.put(ucA.getColumn("A"), bTable.getColumn("D"));
        bTable.addForeignKey("FK_B_D", ucA, fkDMapping);

        BiMap<UniqueConstraintColumn, Column> fkEfMapping = new BiMap<>();
        fkEfMapping.put(ucBC.getColumn("B"), bTable.getColumn("E"));
        fkEfMapping.put(ucBC.getColumn("C"), bTable.getColumn("F"));
        bTable.addForeignKey("FK_B_EF", ucBC, fkEfMapping);

        new OracleCatalogExporter().export(cat, System.out);
    }
}
