package me.vzhilin.dbrow.search;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.catalog.CatalogFilter;
import me.vzhilin.dbrow.catalog.PrimaryKey;
import me.vzhilin.dbrow.catalog.PrimaryKeyColumn;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.filter.AcceptAny;
import me.vzhilin.dbrow.db.*;

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

            Connection conn = ctx.getConnection();
            final PreparedStatement st;
            try {
                st = conn.prepareStatement(q);
            } catch (SQLException e) {
                throw new QueryException("failed to prepare query", q, e);
            }

            for (int i = 0; i < parameters.size(); i++) {
                Object parameterValue = parameters.get(i); // TODO check types
                try {
                    st.setObject(i + 1, parameterValue);
                } catch (SQLException e) {
                    closeSilently(st);
                    throw new QueryException("unable to set parameter[" + i + "] " + parameterValue, q, e);
                }
            }

            final ResultSet rs;
            try {
                rs = st.executeQuery();
            } catch (SQLException e) {
                closeSilently(st);
                throw new QueryException("unable to execute query", q, e);
            }
            try {
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
                            String name = pkc.getName();

                            try {
                                keyColumns[pkc.getPrimaryKeyIndex()] = rs.getObject(name);
                            } catch (SQLException ex) {
                                throw new QueryException("database error: ResultSet.getObject(" + name + ")", ex);
                            }
                        });

                        Key key = new Key(keyColumns);
                        ObjectKey objectKey = new ObjectKey(table, key);
                        Row r = new Row(ctx, objectKey);
                        try {
                            hasNext = rs.next();
                            if (!hasNext) {
                                closeSilently(rs);
                                closeSilently(st);
                            }

                            return r;
                        } catch (SQLException ex) {
                            throw new QueryException("database error: ResultSet.next()", ex);
                        }
                    }
                };

            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private void closeSilently(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ex) {
            // TODO Logger.error
        }
    }
}
