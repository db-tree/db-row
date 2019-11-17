package me.vzhilin.dbrow.adapter.postgres;

import me.vzhilin.dbrow.adapter.BasicValueAccessor;
import me.vzhilin.dbrow.adapter.RowValue;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresValueAccessor extends BasicValueAccessor {
    @Override
    public RowValue get(ResultSet rs, int index) throws SQLException {
        switch (rs.getMetaData().getColumnTypeName(index)) {
            case "uuid": return getUuid(rs, index);
            case "json": return getVarchar(rs, index);
            default: return super.get(rs, index);
        }
    }

    private RowValue getUuid(ResultSet rs, int index) throws SQLException {
        return getCommonValue(rs, index);
    }
}
