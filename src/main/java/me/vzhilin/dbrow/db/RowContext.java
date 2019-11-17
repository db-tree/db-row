package me.vzhilin.dbrow.db;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.ColumnTypeInfo;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.adapter.RowValue;
import me.vzhilin.dbrow.adapter.ValueAccessor;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.io.Closeable;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public final class RowContext {
    private final QueryRunner runner;
    private final DatabaseAdapter adapter;
    private final Connection connection;
    private final Catalog catalog;

    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private final ValueAccessor accessor;

    public RowContext(Catalog catalog, DatabaseAdapter adapter, Connection connection, QueryRunner runner) {
        this.catalog = catalog;
        this.adapter = adapter;
        this.connection = connection;
        this.runner = runner;
        this.accessor = adapter.getAccessor();
    }

    public synchronized void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public synchronized Object getAttribute(String key) {
        return attributes.get(key);
    }

    public Map<Column, RowValue> fetchValues(ObjectKey key) {
        Table table = key.getTable();

        Collection<String> columnNames = table.getColumnNames()
            .stream()
            .map((Function<String, String>) adapter::qualifiedColumnName)
            .collect(Collectors.toList());

        String columns = Joiner.on(',').join(columnNames);

        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(columns).append(" FROM ");

        String schemaName = table.getSchemaName();
        sb.append(adapter.qualifiedTableName(schemaName, table.getName()));
        sb.append(" WHERE ");

        UniqueConstraint pk = table.getAnyUniqueConstraint();
        int primaryKeyColumnCount = pk.getColumnCount();
        List<String> parts = new ArrayList<>(primaryKeyColumnCount);
        List<Object> params = new ArrayList<>(primaryKeyColumnCount);

        key.forEach((column, value) -> {
            parts.add(adapter.qualifiedColumnName(column.getName()) + " = ? ");
            params.add(value);
        });

        sb.append(Joiner.on(" AND ").join(parts));
        ColumnTypeInfo info = adapter.getInfo();

        Map<Column, RowValue> result = new LinkedHashMap<>();
        try (PreparedStatement st = connection.prepareStatement(sb.toString())) {
            for (int i = 0; i < params.size(); i++) {
                st.setObject(i+1, params.get(i));
            }

            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    int index = 0;
                    for (String columnName: table.getColumnNames()) {
                        ++index;

                        Column column = table.getColumn(columnName);
                        String columnType = column.getDataType();

                        RowValue v = accessor.get(rs, index);
                        result.put(column, v);
//                        result.put(column)

//                        long dataLength = probeDataLength(rs, index);
//                        if (dataLength > 256) {
//                            result.put(column, "[" + columnType + ": length " + dataLength + "]");
//                        }
//
//                        ColumnTypeDescription type = info.getType(columnType);
//                        if (isAcceptable(type.getType())) {
//                            result.put(column, rs.getObject(index));
//                        } else {
//                            result.put(column,"[" + columnType + "]");
//                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return result;
    }

//    private long probeDataLength(ResultSet rs, int index) throws SQLException {
//        ResultSetMetaData md = rs.getMetaData();
//        int sqlType = md.getColumnType(index);
////        if (sqlType == Types. || sqlType == Types.CLOB || sqlType == Types.LONGVARCHAR || sqlType == Types.LONGNVARCHAR || sqlType == Types.LONGVARBINARY ||
////        )
//
//
//        Long dataLength = probeBlob(rs, index);
//        if (dataLength != null) {
//            return dataLength;
//        }
//
//        dataLength = probeClob(rs, index);
//        if (dataLength != null) {
//            return dataLength;
//        }
//        return 0;
//    }
//
//    private Long probeBlob(ResultSet rs, int index) throws SQLException {
//        Blob blob = null;
//        try {
//            blob = rs.getBlob(index);
//            if (blob != null) {
//                return blob.length();
//            }
//        } finally {
//            if (blob != null) {
//                blob.free();
//            }
//        }
//        return null;
//    }
//
//    private Long probeClob(ResultSet rs, int index) throws SQLException {
//        Clob clob = null;
//        try {
//            clob = rs.getClob(index);
//            if (clob != null) {
//                return clob.length();
//            }
//            clob = rs.getNClob(index);
//            if (clob != null) {
//                return clob.length();
//            }
//        } finally {
//            if (clob != null) {
//                clob.free();
//            }
//        }
//        return null;
//    }


//    private boolean isAcceptable(ColumnType type) {
//        return acceptableTypes.contains(type);
//    }

    public Number backReferencesCount(Row row, ForeignKey foreignKey) {
        Table table = foreignKey.getTable();
        StringBuilder query = new StringBuilder("SELECT COUNT(1) FROM ");

        query.append(adapter.qualifiedTableName(table));
        query.append(" WHERE ");

        Map<UniqueConstraintColumn, RowValue> vs = row.getKeyValues();
        BiMap<UniqueConstraintColumn, ForeignKeyColumn> mapping = foreignKey.getColumnMapping();

        List<String> parts = new ArrayList<>(vs.size());
        List<Object> params = new ArrayList<>(vs.size());
        vs.forEach((pkColumn, value) -> {
            ForeignKeyColumn fkColumn = mapping.get(pkColumn);
            parts.add(adapter.qualifiedColumnName(fkColumn.getColumn().getName()) + " = ? ");
            params.add(value.get());
        });

        query.append(Joiner.on(" AND ").join(parts));
        try {
            return runner.query(connection, query.toString(), new ScalarHandler<>(), params.toArray());
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @param key row key
     * @return true if row exists
     */
    public boolean exists(ObjectKey key) {
        Table table = key.getTable();
        UniqueConstraint uniq = key.getCons();

        StringBuilder query = new StringBuilder("SELECT COUNT(1) FROM ");

        query.append(adapter.qualifiedTableName(table));
        query.append(" WHERE ");

        Collection<UniqueConstraintColumn> keyColumns = uniq.getColumns();
        List<String> parts = new ArrayList<>(keyColumns.size());
        List<Object> params = new ArrayList<>(keyColumns.size());
        for (UniqueConstraintColumn pkc: keyColumns) {
            parts.add(adapter.qualifiedColumnName(pkc.getName()) + " = ?");
            params.add(key.getKey().get(pkc));
        }
        query.append(Joiner.on(" AND ").join(parts));
        String sql = query.toString();
        try {
            Number c = runner.query(connection, sql, new ScalarHandler<>(), params.toArray());
            return c.longValue() > 0;
        } catch (SQLException e) {
            throw new QueryException("Database error", sql, e);
        }
    }

    public Iterable<Row> backReferences(Row pkRow, ForeignKey fk) {
        return new RowIterable(pkRow, fk);
    }

    public List<Row> selectRows(List<ObjectKey> keys) {
        List<Row> result = new ArrayList<>(keys.size());
        for (ObjectKey k: keys) {
            result.add(new Row(this, k));
        }
        return result;
    }

    public QueryRunner getRunner() {
        return runner;
    }

    public DatabaseAdapter getAdapter() {
        return adapter;
    }

    public Connection getConnection() {
        return connection;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    private final class RowIterable implements Iterable<Row> {
        private final List<RowValue> params;
        private final String query;
        private final Table fkTable;

        private RowIterable(Row pkRow, ForeignKey fk) {
            StringBuilder queryBuilder = new StringBuilder("SELECT ");
            Table fkTable = fk.getTable();

            params = new ArrayList<>();
            List<String> expressions = new ArrayList<>();
            fk.getColumnMapping().forEach((pkColumn, fkColumn) -> {
                expressions.add(adapter.qualifiedColumnName(fkColumn.getColumn().getName()) + " = ?");
                params.add(pkRow.get(pkColumn.getColumn()));
            });

            List<String> pkColumns =
                fk.getTable()
                    .getAnyUniqueConstraint()
                    .getColumnNames()
                    .stream()
                    .map((Function<String, String>) adapter::qualifiedColumnName)
                    .collect(Collectors.toList());

            queryBuilder.append(Joiner.on(", ").join(pkColumns));
            queryBuilder.append(" FROM ").append(adapter.qualifiedTableName(fkTable));
            queryBuilder.append(" WHERE ").append(Joiner.on(" AND ").join(expressions));

            this.fkTable = fkTable;
            this.query = queryBuilder.toString();
        }

        @Override
        public Iterator<Row> iterator() {
            ResultSet rs;
            PreparedStatement st;
            try {
                st = connection.prepareStatement(query);
            } catch (SQLException ex) {
                throw new QueryException("failed to prepare statement", query, ex);
            }

            for (int i = 0; i < params.size(); i++) {
                RowValue p = params.get(i);
                Object parameterValue = null;
                if (p != null) {
                    parameterValue = p.get();
                }

                try {
                    st.setObject(i + 1, parameterValue);
                } catch (SQLException ex) {
                    closeSilently(st);
                    throw new QueryException("unable to set parameter[" + i + "] " + parameterValue, query, ex);
                }
            }

            try {
                rs = st.executeQuery();
            } catch (SQLException ex) {
                throw new QueryException("unable to execute query", query, ex);
            }

            return new RowIterator(fkTable, rs, connection, st); // FIXME ANY cons
        }
    }

    private void closeSilently(AutoCloseable c) {
        try {
            c.close();
        } catch (Exception ex) {
            // TODO Logger.error
        }
    }

    private final class RowIterator implements Iterator<Row>, Closeable {
        private final ResultSet rs;
        private final Set<UniqueConstraintColumn> pkColumns;
        private final Table pkTable;
        private final Connection conn;
        private final Statement st;
        private boolean hasNext;

        private RowIterator(Table pkTable, ResultSet rs, Connection connection, Statement st) {
            this.conn = connection;
            this.st = st;
            this.rs = rs;
            this.pkColumns = pkTable.getAnyUniqueConstraint().getColumns();
            this.pkTable = pkTable;
            try {
                hasNext = rs.next();
            } catch (SQLException e) {
                throw new QueryException("database error", e);
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Row next() {
            UniqueConstraint pk = pkTable.getAnyUniqueConstraint();
            Map<UniqueConstraintColumn, Object> kv = new HashMap<>();

            pkColumns.forEach(ucc -> {
                try {
                    kv.put(ucc, rs.getObject(ucc.getName()));
                } catch (SQLException e) {
                    throw new QueryException("database error: ResultSet.getObject(" + ucc + ")", e);
                }
            });
            ObjectKey key = new ObjectKey(pk, kv);
            try {
                hasNext = rs.next();
            } catch (SQLException e) {
                close();
                throw new QueryException("database error: ResultSet.next()", e);
            }
            if (!hasNext) {
                close();
            }
            return new Row(RowContext.this, key);
        }

        @Override
        public void close() {
            closeSilently(rs);
            closeSilently(st);
        }
    }
}
