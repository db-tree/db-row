package me.vzhilin.search;

import com.google.common.base.Joiner;
import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.Catalog;
import me.vzhilin.catalog.CatalogFilter;
import me.vzhilin.catalog.Table;
import me.vzhilin.catalog.filter.AcceptAny;
import me.vzhilin.db.QueryException;
import me.vzhilin.db.RowContext;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CountOccurences {
    private final QueryRunner runner;
    private final Catalog catalog;
    private final DatabaseAdapter databaseAdapter;
    private final RowContext ctx;
    private final String text;

    public CountOccurences(RowContext ctx, String text) {
        this.text = text;
        this.ctx = ctx;
        this.catalog = ctx.getCatalog();
        this.runner = ctx.getRunner();
        this.databaseAdapter = ctx.getAdapter();
    }

    public Map<Table, Long> count() {
        return count(new AcceptAny());
    }

    public Map<Table, Long> count(CatalogFilter filter) {
        List<Table> tables = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        List<Object> parameterValues = new ArrayList<>();

        if (text == null || text.isEmpty()) {
            countAll(tables, queries, filter);
        } else {
            count(tables, queries, parameterValues, filter);
        }

        Map<Table, Long> result = new LinkedHashMap<>();
        if (tables.isEmpty()) {
            return result;
        }
        String query = Joiner.on(" UNION ALL ").join(queries);
        try {
            for (Map<String, Object> m: runner.query(ctx.getConnection(), query, new MapListHandler(), parameterValues.toArray())) {
                Number n = (Number) m.get("N");
                Number c = (Number) m.get("C");
                Table table = tables.get(n.intValue());
                long count = c.longValue();
                if (count > 0) {
                    result.put(table, count);
                }
            }
            return result;
        } catch (SQLException ex) {
            throw new QueryException("query failed", query, ex);
        }
    }

    private void count(List<Table> tables, List<String> queries, List<Object> parameterValues, CatalogFilter filter) {
        catalog.forEachTable(table -> {
            if (filter.acceptTable(table.getSchemaName(), table.getName())) {
                int n = tables.size();
                String tableName = databaseAdapter.qualifiedTableName(table);
                String exp = buildExpressions(table, parameterValues, filter);
                if (!exp.isEmpty()) {
                    queries.add(String.format("select %d as N, COUNT(*) as C FROM %s WHERE %s", n, tableName, exp));
                    tables.add(table);
                }
            }
        });
    }

    private void countAll(List<Table> tables, List<String> queries, CatalogFilter filter) {
        catalog.forEachTable(table -> {
            if (filter.acceptTable(table.getSchemaName(), table.getName())) {
                int n = tables.size();
                String tableName = databaseAdapter.qualifiedTableName(table);
                queries.add(String.format("select %d as N, COUNT(*) as C FROM %s", n, tableName));
                tables.add(table);
            }
        });
    }

    private String buildExpressions(Table table, List<Object> parameterValues, CatalogFilter filter) {
        final List<String> expressions = new ArrayList<>();
        final ValueConverter conv = databaseAdapter.getConverter();
        table.getColumns().forEach((name, column) -> {
            if (filter.acceptColumn(column.getSchema(), column.getTableName(), column.getName())) {
                Object v;
                if ((v = conv.fromString(text, column.getDataType())) != null) {
                    expressions.add(name + " = ?"); // TODO escape column name
                    parameterValues.add(v);
                }
            }
        });
        return Joiner.on(" OR ").join(expressions);
    }
}
