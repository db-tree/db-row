package me.vzhilin.dbrow.db.misc.conv;

import me.vzhilin.dbrow.adapter.ColumnTypeInfo;
import me.vzhilin.dbrow.adapter.RowValue;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.Column;
import me.vzhilin.dbrow.catalog.TableId;
import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.ObjectKeyBuilder;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ValueConversionTest extends BaseTest {
    @Override
    protected List<TableId> usedTables() {
        return Collections.singletonList(new TableId(currentSchema, s("a")));
    }

    @ParameterizedTest
    @ArgumentsSource(ValueConversionProvider.class)
    public void testParams(CatalogTestEnvironment env, Object[][] expressions) throws SQLException {
        setupEnv(env);
        cleanup();

        ColumnTypeInfo info = adapter.getInfo();
        String numericType = env.getNumberColumnType();

        int id = 1;
        final String tableName = adapter.qualifiedTableName(currentSchema, s("a"));
        for (Object[] e: expressions) {
            String dataType = (String) e[0];
            Object value = e[1];
            String expectedValue = (String) e[2];

            int rowID = id++;
            try (Connection conn = ds.getConnection()) {
                executeCommands("create table " + tableName + " (" +
                        "id " + numericType + ", " +
                        "v " + dataType + "," +
                        "CONSTRAINT a_pk PRIMARY KEY (id)" +
                        ")");

                try(PreparedStatement st = conn.prepareStatement("insert into " + tableName + " (id, v) values (?,?)")) {
                    st.setInt(1, rowID);
                    st.setObject(2, value);
                    st.execute();
                }

                Catalog catalog = loadCatalog(currentSchema);
                UniqueConstraint uc = catalog.getSchema(currentSchema).getTable(s("a")).getAnyUniqueConstraint();

                RowContext ctx = new RowContext(catalog, adapter, conn, new QueryRunner());
                Row r = new Row(ctx, new ObjectKeyBuilder(uc).set(s("id"), rowID).build());
                Column column = catalog.getSchema(currentSchema).getTable(s("a")).getColumn(s("v"));

                RowValue rawValue = r.get(column);
                assertEquals(expectedValue, rawValue.toString());
            } finally {
                executeCommandsSafely("drop table " + tableName);
            }
        }
    }
}
