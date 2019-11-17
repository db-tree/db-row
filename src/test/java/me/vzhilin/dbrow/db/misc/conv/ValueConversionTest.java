package me.vzhilin.dbrow.db.misc.conv;

import me.vzhilin.dbrow.adapter.ColumnTypeInfo;
import me.vzhilin.dbrow.adapter.RowValue;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.Column;
import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.ObjectKeyBuilder;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;
import me.vzhilin.dbrow.db.adapter.TestDatabaseAdapter;
import me.vzhilin.dbrow.db.catalog.CatalogTestEnvironment;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ValueConversionTest extends BaseTest {
    private static final String C_USER_01 = "C##USER01";
    private static final String USER = "USER";

    @ParameterizedTest
    @ArgumentsSource(ValueConversionProvider.class)
    public void testParams(CatalogTestEnvironment env, Object[][] expressions) throws SQLException {
        setupEnv(env);

        TestDatabaseAdapter testAdapter = env.getTestAdapter();
        testAdapter.createUser(C_USER_01, USER);
        try {
            DataSource root = testAdapter.getDataSource();
            DataSource user01 = testAdapter.deriveDatasource(C_USER_01, USER);

            ColumnTypeInfo info = adapter.getInfo();
            String numericType = env.getNumberColumnType();

            int id = 1;
            final String tableName = adapter.qualifiedTableName(C_USER_01, s("a"));
            for (Object[] e: expressions) {
                String dataType = (String) e[0];
                Object value = e[1];
                String expectedValue = (String) e[2];

                int rowID = id++;
                try (Connection conn = user01.getConnection()) {
                    String create = String.format("create table %s (id %s, v %s,CONSTRAINT a_pk PRIMARY KEY (id))", tableName, numericType, dataType);
                    executeCommands(user01, create);

                    try(PreparedStatement st = conn.prepareStatement(String.format("insert into %s (id, v) values (?,?)", tableName))) {
                        st.setInt(1, rowID);
                        st.setObject(2, value);
                        st.execute();
                    }

                    Catalog catalog = loadCatalog(user01, C_USER_01);
                    UniqueConstraint uc = catalog.getSchema(C_USER_01).getTable(s("a")).getAnyUniqueConstraint();

                    RowContext ctx = new RowContext(catalog, adapter, conn, new QueryRunner());
                    Row r = new Row(ctx, new ObjectKeyBuilder(uc).set(s("id"), rowID).build());
                    Column column = catalog.getSchema(C_USER_01).getTable(s("a")).getColumn(s("v"));

                    RowValue rawValue = r.get(column);
                    assertEquals(expectedValue, rawValue.toString());
                } finally {
                    executeCommandsSafely(user01,"drop table " + tableName);
                }
            }
        } finally {
            testAdapter.teardown();
        }
    }
}
