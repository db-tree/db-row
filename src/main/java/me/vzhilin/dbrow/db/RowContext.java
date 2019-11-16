package me.vzhilin.dbrow.db;

import com.google.common.base.Joiner;
import me.vzhilin.dbrow.adapter.ColumnType;
import me.vzhilin.dbrow.adapter.ColumnTypeDescription;
import me.vzhilin.dbrow.adapter.ColumnTypeInfo;
import me.vzhilin.dbrow.adapter.DatabaseAdapter;
import me.vzhilin.dbrow.catalog.*;
import me.vzhilin.dbrow.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.io.Closeable;
import java.sql.*;
import java.util.*;

public final class RowContext {
    private final QueryRunner runner;
    private final DatabaseAdapter adapter;
    private final Connection connection;
    private final Catalog catalog;

    private final Map<String, Object> attributes = new LinkedHashMap<>();
    private final Set<Object> acceptableTypes;

    {
        acceptableTypes = new HashSet<>();
        acceptableTypes.add(ColumnType.INTEGER);
        acceptableTypes.add(ColumnType.FLOAT);
        acceptableTypes.add(ColumnType.DECIMAL);
        acceptableTypes.add(ColumnType.BOOLEAN);
        acceptableTypes.add(ColumnType.DATE);
        acceptableTypes.add(ColumnType.STRING);
    }

    public RowContext(Catalog catalog, DatabaseAdapter adapter, Connection connection, QueryRunner runner) {
        this.catalog = catalog;
        this.adapter = adapter;
        this.connection = connection;
        this.runner = runner;
    }

    public synchronized void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public synchronized Object getAttribute(String key) {
        return attributes.get(key);
    }

    public Map<Column, Object> fetchValues(ObjectKey key) {
        Table table = key.getTable();

        Set<String> columnNames = table.getColumns().keySet();
        String columns = Joiner.on(',').join(columnNames); // todo escape?

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
            parts.add(column.getName() + " = ? ");
            params.add(value);
        });

        sb.append(Joiner.on(" AND ").join(parts));
        ColumnTypeInfo info = adapter.getInfo();

        Map<Column, Object> result = new LinkedHashMap<>();
        try (PreparedStatement st = connection.prepareStatement(sb.toString())) {
            for (int i = 0; i < params.size(); i++) {
                st.setObject(i+1, params.get(i));
            }

            try (ResultSet rs = st.executeQuery()) {
                while (rs.next()) {
                    int index = 0;
                    for (String columnName: columnNames) {
                        ++index;

                        Column column = table.getColumn(columnName);
                        String columnType = column.getDataType();

                        ColumnTypeDescription type = info.getType(columnType);
                        if (isAcceptable(type.getType())) {
                            result.put(column, rs.getObject(index));
                        } else {
                            result.put(column,"[" + columnType + "]");
                        }
                    }
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return result;
    }

    private boolean isAcceptable(ColumnType type) {
        return acceptableTypes.contains(type);
    }

    public Number backReferencesCount(Row row, ForeignKey foreignKey) {
        Table table = foreignKey.getTable();
        StringBuilder query = new StringBuilder("SELECT COUNT(1) FROM ");

        query.append(adapter.qualifiedTableName(table));
        query.append(" WHERE ");

        Map<UniqueConstraintColumn, Object> vs = row.getKeyValues();
        BiMap<UniqueConstraintColumn, ForeignKeyColumn> mapping = foreignKey.getColumnMapping();

        List<String> parts = new ArrayList<>(vs.size());
        List<Object> params = new ArrayList<>(vs.size());
        vs.forEach((pkColumn, value) -> {
            ForeignKeyColumn fkColumn = mapping.get(pkColumn);
            parts.add(fkColumn.getColumn().getName() + " = ? ");
            params.add(value);
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
            parts.add(pkc.getName() + " = ?");
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
        private final List<Object> params;
        private final String query;
        private final Table fkTable;

        private RowIterable(Row pkRow, ForeignKey fk) {
            StringBuilder queryBuilder = new StringBuilder("SELECT ");
            Table fkTable = fk.getTable();

            params = new ArrayList<>();
            List<String> expressions = new ArrayList<>();
            fk.getColumnMapping().forEach((pkColumn, fkColumn) -> {
                expressions.add(fkColumn.getColumn().getName() + " = ?");
                params.add(pkRow.get(pkColumn.getColumn()));
            });

            Set<String> pkColumns = fk.getTable().getAnyUniqueConstraint().getColumnNames();
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
                Object parameterValue = params.get(i);
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
