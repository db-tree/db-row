package me.vzhilin.dbrow.db.catalog;

import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.adapter.TestDatabaseAdapter;
import me.vzhilin.dbrow.util.BiMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.sql.DataSource;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class CatalogTest extends BaseTest {
    @ParameterizedTest
    @ArgumentsSource(CatalogTestArgumentsProvider.class)
    public void testCatalogLoader(CatalogTestEnvironment env) throws SQLException {
        setupEnv(env);
        TestDatabaseAdapter testAdapter = env.getTestAdapter();
        try {
            testAdapter.createUser("C##USER01", "USER");
            DataSource root = testAdapter.getDataSource();
            DataSource user01 = testAdapter.deriveDatasource("C##USER01", "USER");

            Catalog sample = prepareCatalog(env.getNumberColumnType());
            createTables(root, sample);

            Catalog result = loadCatalog(user01, "C##USER01");
            assertEquals(sample, result);
        } finally {
            testAdapter.teardown();
        }
    }

    protected Catalog prepareCatalog(String numberType) {
        Catalog catalog = new Catalog();
        Schema schema = catalog.addSchema("C##USER01");
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
