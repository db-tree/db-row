package me.vzhilin.dbrow.db.row;

import com.google.common.collect.Iterables;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.ObjectKey;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;
import me.vzhilin.dbrow.db.catalog.CatalogTestArgumentsProvider;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RowTests extends BaseTest {
    @Override
    protected List<String> usedTables() {
        return Arrays.asList(s("A"), s("B"), s("C"));
    }

    @ParameterizedTest
    @ArgumentsSource(CatalogTestArgumentsProvider.class)
    public void testReferences(CatalogTestEnvironment env) throws SQLException {
        setupEnv(env);

        Catalog catalog = getCatalog(env.getNumberColumnType());
        createTables(catalog);

        executeCommands(
            "INSERT INTO B(B1, B2) VALUES (200, 400);" +
            "INSERT INTO B(B1, B2) VALUES (201, 401);" +
            "INSERT INTO B(B1, B2) VALUES (202, 402);" +
            "INSERT INTO B(B1, B2) VALUES (203, 403);"
        );

        executeCommands(
            "INSERT INTO C(C1, C2) VALUES (200, 405);" +
            "INSERT INTO C(C1, C2) VALUES (201, 406);" +
            "INSERT INTO C(C1, C2) VALUES (202, 407);" +
            "INSERT INTO C(C1, C2) VALUES (203, 408);"
        );

        executeCommands(
            "INSERT INTO A(A1, A2, A3) VALUES (100, 200, 409);" +
            "INSERT INTO A(A1, A2, A3) VALUES (101, 200, 410);" +
            "INSERT INTO A(A1, A2, A3) VALUES (102, 201, 411);" +
            "INSERT INTO A(A1, A2, A3) VALUES (103, 201, 412);"
        );

        Schema schema = catalog.getSchema(s(currentSchema));
        try (Connection conn = ds.getConnection()) {
            RowContext ctx = new RowContext(catalog, adapter, conn, new QueryRunner());
            Table a = schema.getTable(s("A"));
            Table b = schema.getTable(s("B"));
            Table c = schema.getTable(s("C"));

            Row a100 = getA(ctx, 100);
            Row a101 = getA(ctx, 101);

            ForeignKey fkB = a.getForeignKey(s("FK_B"));
            ForeignKey fkC = a.getForeignKey(s("FK_C"));
            Row b200 = a100.forwardReference(fkB);
            Row c200 = a100.forwardReference(fkC);

            assertEquals(new BigDecimal(400), b200.get(s("B2")));
            assertEquals(new BigDecimal(405), c200.get(s("C2")));

            Map<UniqueConstraint, Map<ForeignKey, Number>> b200BackReferences = b200.backwardReferencesCount();
            UniqueConstraint ucB = b.getAnyUniqueConstraint();
            assertEquals(2, b200BackReferences.get(ucB).get(fkB).intValue());

            assertTrue(Iterables.contains(b200.backwardReference(fkB), a100));
            assertTrue(Iterables.contains(b200.backwardReference(fkB), a101));
        }

        cleanup();
    }

    private Row getA(RowContext ctx, int a1) {
        Map<UniqueConstraintColumn, Object> key = new HashMap<>();
        Table aTable = ctx.getCatalog().getOnlySchema().getTable(s("A"));
        UniqueConstraint ucA = aTable.getAnyUniqueConstraint();
        key.put(ucA.getColumn(s("A1")), new BigDecimal(a1));
        return new Row(ctx, new ObjectKey(ucA, key));
    }

    private Catalog getCatalog(String columnType) {
        Catalog catalog = new Catalog();
        Schema schema = catalog.addSchema(s(currentSchema));
        Table aTable = schema.addTable(s("A"));
        aTable.addColumn(s("A1"), columnType);
        aTable.addColumn(s("A2"), columnType);
        aTable.addColumn(s("A3"), columnType);
        aTable.addUniqueConstraint(s("UC_A"), new String[]{s("A1")});

        Table bTable = schema.addTable(s("B"));
        bTable.addColumn(s("B1"), columnType);
        bTable.addColumn(s("B2"), columnType);
        bTable.addColumn(s("B3"), columnType);
        UniqueConstraint ucB = bTable.addUniqueConstraint(s("UC_B"), new String[]{s("B1")});

        Table cTable = schema.addTable(s("C"));
        cTable.addColumn(s("C1"), columnType);
        cTable.addColumn(s("C2"), columnType);
        cTable.addColumn(s("C3"), columnType);
        UniqueConstraint ucC = cTable.addUniqueConstraint(s("UC_C"), new String[]{s("C1")});

        BiMap<UniqueConstraintColumn, Column> fkB = new BiMap<>();
        fkB.put(ucB.getColumn(s("B1")), aTable.getColumn(s("A2")));
        ForeignKey fkbKey = aTable.addForeignKey(s("FK_B"), ucB, fkB);
        ucB.addForeignKey(fkbKey);

        BiMap<UniqueConstraintColumn, Column> fkC = new BiMap<>();
        fkC.put(ucC.getColumn(s("C1")), aTable.getColumn(s("A2")));
        ForeignKey fkcKey = aTable.addForeignKey(s("FK_C"), ucC, fkC);
        ucC.addForeignKey(fkcKey);
        return catalog;
    }
}