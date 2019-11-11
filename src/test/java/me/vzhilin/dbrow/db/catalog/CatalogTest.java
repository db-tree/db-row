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
    protected List<String> usedTables() {
        return Arrays.asList(s("B"), s("A"));
    }

    @ParameterizedTest
    @ArgumentsSource(CatalogTestArgumentsProvider.class)
    public void testCatalogLoader(CatalogTestEnvironment env) throws SQLException {
        setupEnv(env);

        Catalog sample = prepareCatalog(env.getNumberColumnType());
        createTables(sample);

        Catalog result = loadCatalog();
        assertEquals(sample, result);
        cleanup();
    }


    protected Catalog prepareCatalog(String numberType) {
        Catalog catalog = new Catalog();
        Schema schema = catalog.addSchema(s(currentSchema));
        Table aTable = schema.addTable(s("A"));
        aTable.addColumn(s("A"), numberType, 0);
        aTable.addColumn(s("B"), numberType, 1);
        aTable.addColumn(s("C"), numberType, 2);

        UniqueConstraint ucA = aTable.addUniqueConstraint(s("UC_TEST_A"), new String[]{s("A")});
        UniqueConstraint ucBC = aTable.addUniqueConstraint(s("UC_TEST_BC"), new String[]{s("B"), s("C")});

        Table bTable = schema.addTable(s("B"));
        bTable.addColumn(s("D"), numberType, 0);
        bTable.addColumn(s("E"), numberType, 1);
        bTable.addColumn(s("F"), numberType, 2);

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
