package me.vzhilin.dbrow.search;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.ValueConverter;
import me.vzhilin.dbrow.catalog.CatalogFilter;
import me.vzhilin.dbrow.catalog.Table;
import me.vzhilin.dbrow.catalog.UniqueConstraint;
import me.vzhilin.dbrow.catalog.UniqueConstraintColumn;
import me.vzhilin.dbrow.catalog.filter.AcceptAny;
import me.vzhilin.dbrow.db.ObjectKey;
import me.vzhilin.dbrow.db.QueryException;
import me.vzhilin.dbrow.db.Row;
import me.vzhilin.dbrow.db.RowContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

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
            UniqueConstraint pk = table.getAnyUniqueConstraint();
            for (UniqueConstraintColumn pkc: pk.getColumns()) {
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
                        Map<UniqueConstraintColumn, Object> key = new HashMap<>();
                        pk.getColumns().forEach(pkc -> {
                            try {
                                key.put(pkc, rs.getObject(pkc.getName()));
                            } catch (SQLException ex) {
                                throw new QueryException("database error: ResultSet.getObject(" + pkc.getName() + ")", ex);
                            }
                        });


                        ObjectKey objectKey = new ObjectKey(pk, key);
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
