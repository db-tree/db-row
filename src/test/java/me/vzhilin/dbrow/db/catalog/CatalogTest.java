package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.util.BiMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CatalogTest extends BaseTest {
    @Override
    protected List<TableId> usedTables() {
        TableId b = new TableId(s("C##TEST"), s("B"));
        TableId a = new TableId(s("C##TEST"), s("A"));
        return Arrays.asList(b, a);
    }

    @ParameterizedTest
    @ArgumentsSource(CatalogTestArgumentsProvider.class)
    public void testCatalogLoader(CatalogTestEnvironment env) throws SQLException {
        setupEnv(env);

        Catalog sample = prepareCatalog(env.getNumberColumnType());
        createTables(sample);

        Catalog result = loadCatalog(s("C##TEST"));
        assertEquals(sample, result);
        cleanup();
    }

    protected Catalog prepareCatalog(String numberType) {
        Catalog catalog = new Catalog();
        Schema schema = catalog.addSchema(s("C##TEST"));
        Table aTable = schema.addTable(s("A"));
        aTable.addColumn(s("A"), numberType);
        aTable.addColumn(s("B"), numberType);
        aTable.addColumn(s("C"), numberType);

        UniqueConstraint ucA = aTable.addUniqueConstraint(s("UC_TEST_A"), new String[]{s("A")});
        UniqueConstraint ucBC = aTable.addUniqueConstraint(s("UC_TEST_BC"), new String[]{s("B"), s("C")});

        Table bTable = schema.addTable(s("B"));
        bTable.addColumn(s("D"), numberType);
        bTable.addColumn(s("E"), numberType);
        bTable.addColumn(s("F"), numberType);

        BiMap<UniqueConstraintColumn, Column> fkDMapping = new BiMap<>();
        fkDMapping.put(ucA.getColumn(s("A")), bTable.getColumn(s("D")));
        bTable.addForeignKey(s("FK_B_D"), ucA, fkDMapping);

        BiMap<UniqueConstraintColumn, Column> fkEfMapping = new BiMap<>();
        fkEfMapping.put(ucBC.getColumn(s("B")), bTable.getColumn(s("E")));
        fkEfMapping.put(ucBC.getColumn(s("C")), bTable.getColumn(s("F")));
        bTable.addForeignKey(s("FK_B_EF"), ucBC, fkEfMapping);
        return catalog;
    }
}
