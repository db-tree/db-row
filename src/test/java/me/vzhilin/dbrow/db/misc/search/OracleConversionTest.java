package me.vzhilin.dbrow.db.misc.search;

import me.vzhilin.dbrow.adapter.ColumnTypeInfo;
import me.vzhilin.dbrow.adapter.Converter;
import me.vzhilin.dbrow.catalog.Catalog;
import me.vzhilin.dbrow.catalog.Column;
import me.vzhilin.dbrow.catalog.TableId;
import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.db.BaseTest;
import me.vzhilin.dbrow.db.ObjectKeyBuilder;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;
import me.vzhilin.dbrow.db.env.OracleTestEnvironment;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OracleConversionTest extends BaseTest {
    @Override
    protected List<TableId> usedTables() {
        return Collections.singletonList(new TableId(currentSchema, s("a")));
    }

    @Test
    public void testConversion() throws SQLException {
        OracleTestEnvironment env = new OracleTestEnvironment();
        setupEnv(env);
        cleanup();

        ColumnTypeInfo info = adapter.getInfo();

        Object[][] expressions = new Object[][] {
            {"varchar(4)", "text", "text"},
            {"char(4)", "text", "text"},
            {"nchar(4)", "text", "text"},
            {"NUMBER", "1000", "1000"},
            {"INTEGER", "1000", "1000"},
            {"INT", "1000", "1000"},
            {"SMALLINT", "1000", "1000"},
            {"FLOAT", "1000", "1000"},
            {"REAL", "1000", "1000"},
            {"DOUBLE PRECISION", "1000", "1000"},
            {"BINARY_FLOAT", "1000", "1000.0"},
            {"BINARY_DOUBLE", "1000", "1000.0"},
            {"DATE", java.sql.Date.valueOf("1990-03-15"), "1990-03-15 00:00:00.0"},
//            {"TIMESTAMP", java.sql.Timestamp.valueOf("1990-03-15 04:05:06"), "1990-03-15 04:05:06"},
            {"CLOB", "1000", "[CLOB]"},
            {"BLOB", "1000", "[BLOB]"},
//            {"BFILE", "1000", "[BFILE]"},
            {"RAW(1000)", "1000", "[RAW]"},
            {"LONG RAW", "1000", "[LONG RAW]"},
        };

        int id = 1;

        for (Object[] e: expressions) {
            String dataType = (String) e[0];
            Object value = e[1];
            String expectedValue = (String) e[2];

            int rowID = id++;

            try (Connection conn = ds.getConnection()) {
                executeCommands("create table a(" +
                        "id number, " +
                        "v " + dataType + "," +
                        "CONSTRAINT a_pk PRIMARY KEY (id)" +
                        ")");

                try(PreparedStatement st = conn.prepareStatement("insert into a(id, v) values (?,?)")) {
                    st.setInt(1, rowID);
                    st.setObject(2, value);
                    st.execute();
                }

                Catalog catalog = loadCatalog(currentSchema);
                UniqueConstraint uc = catalog.getSchema(currentSchema).getUniqueConstraint(s("a_pk"));

                RowContext ctx = new RowContext(catalog, adapter, conn, new QueryRunner());
                Row r = new Row(ctx, new ObjectKeyBuilder(uc).set(s("id"), rowID).build());
                Column column = catalog.getSchema(currentSchema).getTable(s("a")).getColumn(s("v"));

                Object rawValue = r.get(column);
                Converter valueConverter = info.getType(column.getDataType()).getConv();
                assertEquals(expectedValue, valueConverter.toString(rawValue));
            } finally {
                executeCommandsSafely("drop table a");
            }
        }
    }
}
