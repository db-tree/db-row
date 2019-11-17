package me.vzhilin.dbrow.db.row;

import com.google.common.collect.Iterables;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.ObjectKey;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;
import me.vzhilin.dbrow.db.adapter.TestDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestArgumentsProvider;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RowTests extends BaseTest {
    private static final String USER = "C##USER01";
    private static final String PW = "USER";

    @ParameterizedTest
    @ArgumentsSource(CatalogTestArgumentsProvider.class)
    public void testReferences(CatalogTestEnvironment env) throws SQLException {
        setupEnv(env);

        TestDatabaseAdapter testAdapter = env.getTestAdapter();
        testAdapter.createUser(USER, PW);
        try {
            DataSource root = testAdapter.getDataSource();
            DataSource user01 = testAdapter.deriveDatasource(USER, PW);

            Catalog catalog = getCatalog(env.getNumberColumnType());
            createTables(user01, catalog);

            String aTable = adapter.qualifiedTableName(USER, "A");
            String bTable = adapter.qualifiedTableName(USER, "B");
            String cTable = adapter.qualifiedTableName(USER, "C");

            String a1 = adapter.qualifiedColumnName("A1");
            String a2 = adapter.qualifiedColumnName("A2");
            String a3 = adapter.qualifiedColumnName("A3");

            String b1 = adapter.qualifiedColumnName("B1");
            String b2 = adapter.qualifiedColumnName("B2");

            String c1 = adapter.qualifiedColumnName("C1");
            String c2 = adapter.qualifiedColumnName("C2");
            executeCommands(user01,
                    String.format("INSERT INTO %s(%s, %s) VALUES (200, 400);", bTable, b1, b2) +
                            String.format("INSERT INTO %s(%s, %s) VALUES (201, 401);", bTable, b1, b2) +
                            String.format("INSERT INTO %s(%s, %s) VALUES (202, 402);", bTable, b1, b2)
            );

            executeCommands(user01,
                    String.format("INSERT INTO %s(%s, %s) VALUES (200, 405);", cTable, c1, c2) +
                            String.format("INSERT INTO %s(%s, %s) VALUES (201, 406);", cTable, c1, c2) +
                            String.format("INSERT INTO %s(%s, %s) VALUES (202, 407);", cTable, c1, c2) +
                            String.format("INSERT INTO %s(%s, %s) VALUES (203, 408);", cTable, c1, c2)
            );

            executeCommands(user01,
                    String.format("INSERT INTO %s(%s, %s, %s) VALUES (100, 200, 409);", aTable, a1, a2, a3) +
                            String.format("INSERT INTO %s(%s, %s, %s) VALUES (101, 200, 410);", aTable, a1, a2, a3) +
                            String.format("INSERT INTO %s(%s, %s, %s) VALUES (102, 201, 411);", aTable, a1, a2, a3) +
                            String.format("INSERT INTO %s(%s, %s, %s) VALUES (103, 201, 412);", aTable, a1, a2, a3)
            );

            Schema schema = catalog.getSchema(USER);
            try (Connection conn = user01.getConnection()) {
                RowContext ctx = new RowContext(catalog, adapter, conn, new QueryRunner());
                Table a = schema.getTable("A");
                Table b = schema.getTable("B");
                Table c = schema.getTable("C");

                Row a100 = getA(ctx, 100);
                Row a101 = getA(ctx, 101);

                ForeignKey fkB = a.getForeignKey("FK_B");
                ForeignKey fkC = a.getForeignKey("FK_C");
                Row b200 = a100.forwardReference(fkB);
                Row c200 = a100.forwardReference(fkC);

                assertEquals(new BigDecimal(400), b200.get("B2").get());
                assertEquals(new BigDecimal(405), c200.get("C2").get());

                Map<UniqueConstraint, Map<ForeignKey, Number>> b200BackReferences = b200.backwardReferencesCount();
                UniqueConstraint ucB = b.getAnyUniqueConstraint();
                assertEquals(2, b200BackReferences.get(ucB).get(fkB).intValue());

                assertTrue(Iterables.contains(b200.backwardReference(fkB), a100));
                assertTrue(Iterables.contains(b200.backwardReference(fkB), a101));
            }
        } finally {
            testAdapter.teardown();
        }
    }

    private Row getA(RowContext ctx, int a1) {
        Map<UniqueConstraintColumn, Object> key = new HashMap<>();
        Table aTable = ctx.getCatalog().getSchema(USER).getTable("A");
        UniqueConstraint ucA = aTable.getAnyUniqueConstraint();
        key.put(ucA.getColumn("A1"), new BigDecimal(a1));
        return new Row(ctx, new ObjectKey(ucA, key));
    }

    private Catalog getCatalog(String columnType) {
        Catalog catalog = new Catalog();
        Schema schema = catalog.addSchema(USER);
        Table aTable = schema.addTable("A");
        aTable.addColumn("A1", columnType);
        aTable.addColumn("A2", columnType);
        aTable.addColumn("A3", columnType);
        aTable.addUniqueConstraint("UC_A", new String[]{"A1"});

        Table bTable = schema.addTable("B");
        bTable.addColumn("B1", columnType);
        bTable.addColumn("B2", columnType);
        bTable.addColumn("B3", columnType);
        UniqueConstraint ucB = bTable.addUniqueConstraint("UC_B", new String[]{"B1"});

        Table cTable = schema.addTable("C");
        cTable.addColumn("C1", columnType);
        cTable.addColumn("C2", columnType);
        cTable.addColumn("C3", columnType);
        UniqueConstraint ucC = cTable.addUniqueConstraint("UC_C", new String[]{"C1"});

        BiMap<UniqueConstraintColumn, Column> fkB = new BiMap<>();
        fkB.put(ucB.getColumn("B1"), aTable.getColumn("A2"));
        ForeignKey fkbKey = aTable.addForeignKey("FK_B", ucB, fkB);
        ucB.addForeignKey(fkbKey);

        BiMap<UniqueConstraintColumn, Column> fkC = new BiMap<>();
        fkC.put(ucC.getColumn("C1"), aTable.getColumn("A2"));
        ForeignKey fkcKey = aTable.addForeignKey("FK_C", ucC, fkC);
        ucC.addForeignKey(fkcKey);
        return catalog;
    }
}
