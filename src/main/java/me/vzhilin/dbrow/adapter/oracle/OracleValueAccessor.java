package me.vzhilin.dbrow.adapter.oracle;

import me.vzhilin.dbrow.adapter.BasicValueAccessor;
import me.vzhilin.dbrow.adapter.RowValue;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleValueAccessor extends BasicValueAccessor {
    @Override
    public RowValue get(ResultSet rs, int index) throws SQLException {
        switch (rs.getMetaData().getColumnTypeName(index)) {
            case "BINARY_FLOAT": return getFloat(rs, index);
            case "BINARY_DOUBLE": return getDouble(rs, index);
            case "TIMESTAMP WITH TIME ZONE": return getTimestamp(rs, index);
            default: return super.get(rs, index);
        }
    }
}
