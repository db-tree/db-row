package me.vzhilin.db;

import com.google.common.base.Joiner;
import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.catalog.*;
import me.vzhilin.util.BiMap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.io.Closeable;
import java.sql.*;
import java.util.*;
import java.util.function.BiConsumer;

public final class RowContext {
    private final QueryRunner runner;
    private final DatabaseAdapter adapter;
    private final Connection connection;
    private final Catalog catalog;

    private final Map<String, Object> attributes = new HashMap<>();

    public RowContext(Catalog catalog, DatabaseAdapter adapter, Connection connection, QueryRunner runner) {
        this.catalog = catalog;
        this.adapter = adapter;
        this.connection = connection;
        this.runner = runner;
    }

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public Object getAttribute(String key, Object value) {
        return attributes.get(key);
    }

    public Map<Column, Object> fetchValues(ObjectKey key) {
        Table table = key.getTable();
        String columns = Joiner.on(',').join(table.getColumns().keySet());
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(columns).append(" FROM ");

        String schemaName = table.getSchemaName();
        if (schemaName != null) {
            sb.append(schemaName + ".");
        }

        sb.append(table.getName());

        sb.append(" WHERE ");
        int primaryKeyColumnCount = table.getPrimaryKey().get().getColumnCount();
        List<String> parts = new ArrayList<>(primaryKeyColumnCount);
        List<Object> params = new ArrayList<>(primaryKeyColumnCount);

        key.forEach(new BiConsumer<PrimaryKeyColumn, Object>() {
            @Override
            public void accept(PrimaryKeyColumn column, Object value) {
                parts.add(column.getName() + " = ? ");
                params.add(value);
            }
        });


        sb.append(Joiner.on(" AND ").join(parts));
        Map<String, Object> rawResult;
        try {
            rawResult = runner.query(connection, sb.toString(), new MapHandler(), params.toArray());
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        Map<Column, Object> result = new HashMap<>(rawResult.size());
        rawResult.forEach((name, o) -> result.put(table.getColumn(name), o));
        return result;
    }

    public Number backReferencesCount(Row row, ForeignKey foreignKey) {
        Table table = foreignKey.getTable();
        String tableName = table.getName();

        StringBuilder query = new StringBuilder("SELECT COUNT(1) FROM ");
        String schemaName = table.getSchemaName();
        if (schemaName != null) {
            query.append(schemaName).append(".");
        }

        query.append(tableName);
        query.append(" WHERE ");

        Map<PrimaryKeyColumn, Object> vs = row.getKeyValues();
        BiMap<PrimaryKeyColumn, ForeignKeyColumn> mapping = foreignKey.getColumnMapping();

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

        public RowIterable(Row pkRow, ForeignKey fk) {
            StringBuilder queryBuilder = new StringBuilder("SELECT ");

            Table pkTable = fk.getPkTable();
            Table fkTable = fk.getTable();

            params = new ArrayList<>();
            List<String> expressions = new ArrayList<>();
            fk.getColumnMapping().forEach((pkColumn, fkColumn) -> {
                expressions.add(fkColumn.getColumn().getName() + " = ?");
                params.add(pkRow.get(pkColumn.getColumn()));
            });

            Set<String> pkColumns = fkTable.getPrimaryKey().get().getColumnNames();
            queryBuilder.append(Joiner.on(", ").join(pkColumns));
            queryBuilder.append(" FROM ").append(fkTable.getName());
            queryBuilder.append(" WHERE ").append(Joiner.on(" AND ").join(expressions));

            this.fkTable = fkTable;
            this.query = queryBuilder.toString();
        }

        @Override
        public Iterator<Row> iterator() {
            try {
                PreparedStatement st = connection.prepareStatement(query);
                for (int i = 0; i < params.size(); i++) {
                    st.setObject(i + 1, params.get(i));
                }
                return new RowIterator(fkTable, st.executeQuery(), connection, st);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private final class RowIterator implements Iterator<Row>, Closeable {
        private final ResultSet rs;
        private final Set<String> pkColumns;
        private final Table pkTable;
        private final Connection conn;
        private final Statement st;
        private boolean hasNext;

        private RowIterator(Table pkTable, ResultSet rs, Connection connection, Statement st) {
            this.conn = connection;
            this.st = st;
            this.rs = rs;
            this.pkColumns = pkTable.getPrimaryKey().get().getColumnNames();
            this.pkTable = pkTable;
            try {
                hasNext = rs.next();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Row next() {
            PrimaryKey pk = pkTable.getPrimaryKey().get();
            Object[] kv = new Object[pk.getColumnCount()];

            pkColumns.forEach(name -> {
                try {
                    kv[pk.getColumn(name).getPrimaryKeyIndex()] = rs.getString(name);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            ObjectKey key = new ObjectKey(pkTable, new Key(kv));
            try {
                hasNext = rs.next();
            } catch (SQLException e) {
                close();
                throw new RuntimeException(e);
            }
            if (!hasNext) {
                close();
            }
            return new Row(RowContext.this, key);
        }

        @Override
        public void close() {
            // TODO proper closing
            try {
                st.close();
                rs.close();
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
