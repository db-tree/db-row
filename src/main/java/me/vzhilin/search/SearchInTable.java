package me.vzhilin.search;

import com.google.common.base.Joiner;
import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.CatalogFilter;
import me.vzhilin.catalog.PrimaryKey;
import me.vzhilin.catalog.PrimaryKeyColumn;
import me.vzhilin.catalog.Table;
import me.vzhilin.catalog.filter.AcceptAny;
import me.vzhilin.db.Key;
import me.vzhilin.db.ObjectKey;
import me.vzhilin.db.Row;
import me.vzhilin.db.RowContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class SearchInTable {
    private final RowContext ctx;
    private final Table table;
    private final String text;

    public SearchInTable(RowContext ctx, Table table, String text) {
        this.ctx = ctx;
        this.table = table;
        this.text = text;
    }

    public Iterable<Row> search() {
        return search(new AcceptAny());
    }

    public Iterable<Row> search(CatalogFilter filter) {
        return new RowIterable(ctx, table, text, filter);
    }

    private final class RowIterable implements Iterable<Row> {
        private final Table table;
        private final RowContext ctx;
        private final String text;
        private final CatalogFilter filter;

        public RowIterable(RowContext ctx, Table table, String text, CatalogFilter filter) {
            this.filter = filter;
            this.table = table;
            this.ctx = ctx;
            this.text = text;
        }

        private String buildExpressions(List<Object> parameterValues) {
            List<String> expressions = new ArrayList<>();
            DatabaseAdapter adapter = ctx.getAdapter();
            final ValueConverter conv = adapter.getConverter();
            table.getColumns().forEach((name, column) -> {
                if (filter.acceptColumn(column.getSchema(), column.getTableName(), column.getName())) {
                    Object v = conv.fromString(text, column.getDataType());
                    if (v != null) {
                        expressions.add(name + " = ?");
                        parameterValues.add(v);
                    }
                }
            });
            return Joiner.on(" OR ").join(expressions);
        }

        @Override
        public Iterator<Row> iterator() { // TODO Closeable iterator
            List<String> pkColumns = new ArrayList<>();
            List<Object> parameters = new ArrayList<>();

            String qualifiedTableName = ctx.getAdapter().qualifiedTableName(table);
            PrimaryKey pk = table.getPrimaryKey().get();
            for (PrimaryKeyColumn pkc: pk.getColumns()) {
                pkColumns.add(pkc.getName());
            }
            String joinedPks = Joiner.on(',').join(pkColumns);
            String exp = buildExpressions(parameters);
            if (parameters.isEmpty()) {
                return Collections.emptyIterator();
            }

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
