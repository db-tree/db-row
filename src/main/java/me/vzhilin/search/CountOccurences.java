package me.vzhilin.search;

import com.google.common.base.Joiner;
import me.vzhilin.adapter.DatabaseAdapter;
import me.vzhilin.adapter.ValueConverter;
import me.vzhilin.catalog.Catalog;
import me.vzhilin.catalog.Table;
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

    public CountOccurences(RowContext ctx) {
        this.ctx = ctx;
        this.catalog = ctx.getCatalog();
        this.runner = ctx.getRunner();
        this.databaseAdapter = ctx.getAdapter();
    }

    public Map<Table, Long> count(String text) {
        List<Table> tables = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        List<Object> parameterValues = new ArrayList<>();

        catalog.forEachTable(table -> {
            int n = tables.size();
            String qualifiedTableName = databaseAdapter.qualifiedTableName(table);
            String exp = buildExpressions(table, parameterValues, text);
            if (!exp.isEmpty()) {
                queries.add(String.format("select %d as N, COUNT(*) as C FROM %s WHERE %s", n, qualifiedTableName, exp));
                tables.add(table);
            }
        });

        try {
            Map<Table, Long> result = new LinkedHashMap<>();
            if (tables.isEmpty()) {
                return result;
            }
            String query = Joiner.on(" UNION ALL ").join(queries);
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
            throw new RuntimeException(ex);
        }
    }

    private String buildExpressions(Table table, List<Object> parameterValues, String text) {
        final List<String> expressions = new ArrayList<>();
        final ValueConverter conv = databaseAdapter.getConverter();

        table.getColumns().forEach((name, column) -> {
            Object v;
            if ((v = conv.fromString(text, column.getDataType())) != null) {
                expressions.add(name + " = ?");
                parameterValues.add(v);
            }
        });
        return Joiner.on(" OR ").join(expressions);
    }
}
