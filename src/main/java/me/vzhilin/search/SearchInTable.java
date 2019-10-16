package me.vzhilin.search;

import com.google.common.base.Joiner;
import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.PrimaryKey;
import me.vzhilin.catalog.PrimaryKeyColumn;
import me.vzhilin.catalog.Table;
import me.vzhilin.db.Key;
import me.vzhilin.db.ObjectKey;
import me.vzhilin.db.Row;
import me.vzhilin.db.RowContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SearchInTable {
    private final RowContext ctx;
    private final Table table;

    public SearchInTable(RowContext ctx, Table table) {
        this.ctx = ctx;
        this.table = table;
    }

    public Iterable<Row> search(String text) {
        return new RowIterable(ctx, table, text);
    }

    private String buildExpressions(Table table, List<Object> parameterValues, String text) {
        List<String> expressions = new ArrayList<>();
        DatabaseAdapter adapter = ctx.getAdapter();
        final ValueConverter conv = adapter.getConverter();
        table.getColumns().forEach((name, column) -> {
            Object v = conv.fromString(text, column.getDataType());
            if (v != null) {
                expressions.add(name + " = ?");
                parameterValues.add(text);
            }
        });
        return Joiner.on(" OR ").join(expressions);
    }

    private final class RowIterable implements Iterable<Row> {
        private final Table table;
        private final RowContext ctx;
        private final String text;

        public RowIterable(RowContext ctx, Table table, String text) {
            this.table = table;
            this.ctx = ctx;
            this.text = text;
        }

        @Override
        public Iterator<Row> iterator() {
            List<String> pkColumns = new ArrayList<>();
            List<Object> parameters = new ArrayList<>();

            String qualifiedTableName = ctx.getAdapter().qualifiedTableName(table);
            PrimaryKey pk = table.getPrimaryKey().get();
            for (PrimaryKeyColumn pkc: pk.getColumns()) {
                pkColumns.add(pkc.getName());
            }
            String joinedPks = Joiner.on(',').join(pkColumns);
            String exp = buildExpressions(table, parameters, text);
            String q = String.format("SELECT %s from %s WHERE %s", joinedPks, qualifiedTableName, exp);

            try {
                Connection conn = ctx.getConnection();
                PreparedStatement st = conn.prepareStatement(q);
                for (int i = 0; i < parameters.size(); i++) {
                    Object param = parameters.get(i); // TODO check types

                    st.setObject(i + 1, param);
                }

                ResultSet rs = st.executeQuery();
                return new Iterator<Row>() {
                    boolean hasNext = rs.next();
                    @Override
                    public boolean hasNext() {
                        return hasNext;
                    }

                    @Override
                    public Row next() {
                        Object[] keyColumns = new Object[pk.getColumnCount()];
                        pk.getColumns().forEach(pkc -> {
                            try {
                                keyColumns[pkc.getPrimaryKeyIndex()] = rs.getObject(pkc.getName());
                            } catch (SQLException ex) {
                                throw new RuntimeException(ex);
                            }
                        });

                        Key key = new Key(keyColumns);
                        ObjectKey objectKey = new ObjectKey(table, key);
                        Row r = new Row(ctx, objectKey);
                        try {
                            hasNext = rs.next();
                            if (!hasNext) {
                                try {
                                    rs.close();
                                    st.close();
//                                    conn.close(); // FIXME REUSE CONNECTION
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            return r;
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                };

            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
